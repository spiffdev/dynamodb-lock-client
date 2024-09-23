import * as crypto from 'crypto';
import * as events from 'events';
import * as os from 'os';
import { GetCommand, PutCommand, DeleteCommand } from '@aws-sdk/lib-dynamodb';

var name = "@spiffcommerce/dynamodb-lock-client";
var version = "1.0.0";

function buildAttributeExistsExpression(sortKey) {
    let expr = "attribute_exists(#partitionKey)";
    if (sortKey) {
        expr += " and attribute_exists(#sortKey)";
        return `(${expr})`;
    }
    return expr;
}
function buildAttributeNotExistsExpression(sortKey) {
    let expr = "attribute_not_exists(#partitionKey)";
    if (sortKey) {
        expr += " and attribute_not_exists(#sortKey)";
        return `(${expr})`;
    }
    return expr;
}
function buildExpressionAttributeNames(partitionKey, sortKey) {
    const names = {
        "#partitionKey": partitionKey,
    };
    if (sortKey) {
        names["#sortKey"] = sortKey;
    }
    return names;
}
function getMsForLease(time, unit) {
    switch (unit) {
        case "milliseconds":
            return time;
        case "seconds":
            return time * 1000;
        case "minutes":
            return time * 1000 * 60;
        case "hours":
            return time * 1000 * 60 * 60;
        case "days":
            return time * 1000 * 60 * 60 * 24;
        default:
            throw new Error(`Invalid lease unit: ${unit}`);
    }
}
class FailOpenError extends Error {
    code;
    originalError;
    constructor(message, code, originalError) {
        super(message);
        this.code = code;
        this.originalError = originalError;
    }
}
class FailOpen {
    config;
    retryCount;
    constructor(config) {
        this.config = config;
        this.retryCount = this.config.retryCount ?? 1;
    }
    acquireLock(id, callback) {
        let partitionID;
        let sortID;
        if (typeof id === "object") {
            partitionID = id[this.config.partitionKey];
            sortID = id[this.config.sortKey];
        }
        else {
            partitionID = id;
        }
        if (this.config.sortKey && sortID === undefined) {
            return callback(new Error("Lock ID is missing required sortKey value"));
        }
        return this.checkForExistingLock({
            partitionID,
            sortID,
            ownerName: this.config.ownerName || `${name}@${version}_${os.userInfo().username}@${os.hostname()}`,
            retryCount: this.retryCount,
            recordVersionNumber: crypto.randomUUID(),
        }, callback);
    }
    checkForExistingLock(dataBag, callback) {
        const params = {
            TableName: this.config.lockTable,
            Key: {
                [this.config.partitionKey]: dataBag.partitionID,
            },
            ConsistentRead: true,
        };
        if (this.config.sortKey) {
            params.Key[this.config.sortKey] = dataBag.sortID;
        }
        this.config.dynamodb.send(new GetCommand(params), (error, data) => {
            if (error) {
                return callback(error);
            }
            if (!data?.Item) {
                return this.acquireNewLock(dataBag, callback);
            }
            // Lease durations are stored in milliseconds in DynamoDB
            const leaseDurationMs = parseInt(data.Item.leaseDuration) || 10000;
            let timeout;
            if (this.config.trustLocalTime) {
                const lockAcquiredTimeUnixMs = parseInt(data.Item.lockAcquiredTimeUnixMs);
                const localTimeUnixMs = new Date().getTime();
                timeout = Math.max(0, leaseDurationMs - (localTimeUnixMs - lockAcquiredTimeUnixMs));
            }
            else {
                timeout = leaseDurationMs;
            }
            return setTimeout(() => this.acquireExistingLock(dataBag, data.Item, callback), timeout);
        });
    }
    acquireNewLock(dataBag, callback) {
        const params = {
            TableName: this.config.lockTable,
            Item: {
                [this.config.partitionKey]: dataBag.partitionID,
                leaseDuration: getMsForLease(this.config.leaseDuration || 10000, this.config.leaseUnit).toString(),
                ownerName: dataBag.ownerName,
                recordVersionNumber: dataBag.recordVersionNumber,
                createdAt: new Date().toISOString(),
            },
            ConditionExpression: buildAttributeNotExistsExpression(this.config.sortKey),
            ExpressionAttributeNames: buildExpressionAttributeNames(this.config.partitionKey, this.config.sortKey),
        };
        if (this.config.trustLocalTime) {
            params.Item["lockAcquiredTimeUnixMs"] = new Date().getTime();
        }
        if (dataBag.sortID) {
            params.Item[this.config.sortKey] = dataBag.sortID;
        }
        return this.tryAcquireLock(params, dataBag, callback);
    }
    acquireExistingLock(dataBag, lock, callback) {
        const params = {
            TableName: this.config.lockTable,
            Item: {
                [this.config.partitionKey]: dataBag.partitionID,
                leaseDuration: getMsForLease(this.config.leaseDuration || 10000, this.config.leaseUnit).toString(),
                ownerName: dataBag.ownerName,
                recordVersionNumber: dataBag.recordVersionNumber,
                createdAt: new Date().toISOString(),
            },
            ConditionExpression: `${buildAttributeNotExistsExpression(this.config.sortKey)} or (recordVersionNumber = :recordVersionNumber)`,
            ExpressionAttributeNames: buildExpressionAttributeNames(this.config.partitionKey, this.config.sortKey),
            ExpressionAttributeValues: {
                ":recordVersionNumber": lock.recordVersionNumber,
            },
        };
        if (this.config.trustLocalTime) {
            params.Item.lockAcquiredTimeUnixMs = new Date().getTime();
        }
        if (dataBag.sortID) {
            params.Item[this.config.sortKey] = dataBag.sortID;
        }
        return this.tryAcquireLock(params, dataBag, callback);
    }
    tryAcquireLock(params, dataBag, callback) {
        this.config.dynamodb.send(new PutCommand(params), (error) => {
            if (error) {
                if (error.code === "ConditionalCheckFailedException") {
                    if (dataBag.retryCount > 0) {
                        dataBag.retryCount--;
                        return this.checkForExistingLock(dataBag, callback);
                    }
                    else {
                        const err = new FailOpenError("Failed to acquire lock.", "FailedToAcquireLock", error);
                        return callback(err);
                    }
                }
                return callback(error);
            }
            return callback(undefined, new Lock({
                dynamodb: this.config.dynamodb,
                recordVersionNumber: dataBag.recordVersionNumber,
                heartbeatPeriodMs: this.config.heartbeatPeriodMs,
                leaseDuration: this.config.leaseDuration || 10000,
                leaseUnit: this.config.leaseUnit,
                lockTable: this.config.lockTable,
                ownerName: dataBag.ownerName,
                partitionID: dataBag.partitionID,
                partitionKey: this.config.partitionKey,
                sortID: dataBag.sortID,
                sortKey: this.config.sortKey,
                trustLocalTime: this.config.trustLocalTime,
            }));
        });
    }
}
class Lock extends events.EventEmitter {
    config;
    recordVersionNumber;
    released;
    refreshing;
    refreshCallback;
    heartbeatTimeout;
    constructor(config) {
        super();
        this.config = config;
        this.recordVersionNumber = this.config.recordVersionNumber;
        this.released = false;
        if (this.config.heartbeatPeriodMs) {
            this.heartbeatTimeout = setTimeout(this.refreshLock.bind(this), this.config.heartbeatPeriodMs);
        }
    }
    release(callback) {
        if (this.released) {
            return;
        }
        try {
            if (this.refreshing) {
                this.refreshCallback = this.release.bind(this, callback);
                return;
            }
            this.released = true;
            if (this.heartbeatTimeout) {
                clearTimeout(this.heartbeatTimeout);
                this.heartbeatTimeout = undefined;
            }
            const params = {
                TableName: this.config.lockTable,
                Key: {
                    [this.config.partitionKey]: this.config.partitionID,
                },
                ConditionExpression: `${buildAttributeExistsExpression(this.config.sortKey)} and recordVersionNumber = :recordVersionNumber`,
                ExpressionAttributeNames: buildExpressionAttributeNames(this.config.partitionKey, this.config.sortKey),
                ExpressionAttributeValues: {
                    ":recordVersionNumber": this.recordVersionNumber,
                },
            };
            if (this.config.sortKey) {
                params.Key[this.config.sortKey] = this.config.sortID;
            }
            this.config.dynamodb.send(new DeleteCommand(params), (error) => {
                if (error && error.code === "ConditionalCheckFailedException") {
                    console.warn("WARNING: Failed to release lock: ConditionalCheckFailedException.");
                }
                return callback(undefined);
            });
        }
        catch (e) {
            console.warn(`WARNING: Failed to release lock: ${e}`);
            callback(undefined);
        }
    }
    refreshLock() {
        this.refreshing = true;
        const newGuid = crypto.randomUUID();
        const params = {
            TableName: this.config.lockTable,
            Item: {
                [this.config.partitionKey]: this.config.partitionID,
                leaseDuration: getMsForLease(this.config.leaseDuration || 10000, this.config.leaseUnit).toString(),
                ownerName: this.config.ownerName,
                recordVersionNumber: newGuid,
                createdAt: new Date().toISOString(),
            },
            ConditionExpression: `${buildAttributeExistsExpression(this.config.sortKey)} and recordVersionNumber = :recordVersionNumber`,
            ExpressionAttributeNames: buildExpressionAttributeNames(this.config.partitionKey, this.config.sortKey),
            ExpressionAttributeValues: {
                ":recordVersionNumber": this.recordVersionNumber,
            },
        };
        if (this.config.trustLocalTime) {
            params.Item.lockAcquiredTimeUnixMs = new Date().getTime();
        }
        if (this.config.sortKey) {
            params.Item[this.config.sortKey] = this.config.sortID;
        }
        this.config.dynamodb.send(new PutCommand(params), (error) => {
            this.refreshing = false;
            if (this.refreshCallback) {
                this.refreshCallback();
            }
            this.refreshCallback = undefined;
            if (error) {
                return this.emit("error", error);
            }
            this.recordVersionNumber = newGuid;
            if (!this.released) {
                this.heartbeatTimeout = setTimeout(this.refreshLock.bind(this), this.config.heartbeatPeriodMs);
            }
        });
    }
}

export { FailOpen, FailOpenError, Lock };
