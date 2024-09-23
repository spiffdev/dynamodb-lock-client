import * as crypto from "crypto";
import * as events from "events";
import * as os from "os";
import * as pkg from "./package.json";
import {
    DynamoDBDocumentClient,
    DeleteCommandInput,
    PutCommandInput,
    PutCommand,
    GetCommand,
    GetCommandInput,
    DeleteCommand,
} from "@aws-sdk/lib-dynamodb";

type LeaseUnit = "milliseconds" | "seconds" | "minutes" | "hours" | "days";

export interface FailOpenConfig {
    ownerName: string;
    dynamodb: DynamoDBDocumentClient;
    lockTable: string;
    partitionKey: string;
    sortKey?: string;
    heartbeatPeriodMs?: number;
    leaseDuration: number;
    leaseUnit: LeaseUnit;
    trustLocalTime?: boolean;
    retryCount?: number;
}

interface LockData extends FailOpenConfig {
    partitionID: string | number;
    recordVersionNumber: string;
    sortID?: string | number;
    heartbeatPeriodMs?: number;
}

function buildAttributeExistsExpression(sortKey: string | undefined): string {
    let expr = "attribute_exists(#partitionKey)";
    if (sortKey) {
        expr += " and attribute_exists(#sortKey)";
        return `(${expr})`;
    }
    return expr;
}

function buildAttributeNotExistsExpression(sortKey: string | undefined): string {
    let expr = "attribute_not_exists(#partitionKey)";
    if (sortKey) {
        expr += " and attribute_not_exists(#sortKey)";
        return `(${expr})`;
    }
    return expr;
}

function buildExpressionAttributeNames(partitionKey: string, sortKey: string | undefined): Record<string, string> {
    const names: Record<string, string> = {
        "#partitionKey": partitionKey,
    };
    if (sortKey) {
        names["#sortKey"] = sortKey;
    }
    return names;
}

function getMsForLease(time: number, unit: string): number {
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

export class FailOpenError extends Error {
    constructor(
        message: string,
        public code: string,
        public originalError: Error,
    ) {
        super(message);
    }
}

interface AcquireLockData {
    partitionID: string | number;
    recordVersionNumber: string;
    sortID?: string | number;
    heartbeatPeriodMs?: number;
    ownerName: string;
    retryCount: number;
}

const reservedFieldNames = ["leaseDuration", "lockAcquiredTimeUnixMs", "ownerName", "recordVersionNumber"];

export class FailOpen<PartitionTableKeyType extends string | number | Record<string, any>> {
    private config: FailOpenConfig;
    private retryCount: number;

    constructor(config: FailOpenConfig) {
        if (reservedFieldNames.includes(config.partitionKey)) {
            throw new Error(`Cannot use reserved field name ${config.partitionKey} for partition key.`);
        }
        if (config.sortKey && reservedFieldNames.includes(config.sortKey)) {
            throw new Error(`Cannot use reserved field name ${config.sortKey} for sort key.`);
        }

        this.config = config;
        this.retryCount = this.config.retryCount ?? 1;
    }

    acquireLock(id: PartitionTableKeyType, callback: (error: Error | undefined, lock?: Lock) => void) {
        let partitionID: string | number;
        let sortID: string | number | undefined;
        if (typeof id === "object") {
            partitionID = id[this.config.partitionKey];
            sortID = id[this.config.sortKey!];
        } else {
            partitionID = id;
        }
        if (this.config.sortKey && sortID === undefined) {
            return callback(new Error("Lock ID is missing required sortKey value"));
        }
        return this.checkForExistingLock(
            {
                partitionID,
                sortID,
                ownerName:
                    this.config.ownerName || `${pkg.name}@${pkg.version}_${os.userInfo().username}@${os.hostname()}`,
                retryCount: this.retryCount,
                recordVersionNumber: crypto.randomUUID(),
            },
            callback,
        );
    }

    private checkForExistingLock(dataBag: AcquireLockData, callback: (error: Error | undefined, lock?: Lock) => void) {
        const params: GetCommandInput = {
            TableName: this.config.lockTable,
            Key: {
                [this.config.partitionKey]: dataBag.partitionID,
            },
            ConsistentRead: true,
        };
        if (this.config.sortKey) {
            params.Key![this.config.sortKey] = dataBag.sortID;
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
            } else {
                timeout = leaseDurationMs;
            }
            return setTimeout(() => this.acquireExistingLock(dataBag, data.Item as LockData, callback), timeout);
        });
    }

    private acquireNewLock(dataBag: AcquireLockData, callback: (error: Error | undefined, lock?: Lock) => void) {
        const params: PutCommandInput = {
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
            params.Item!["lockAcquiredTimeUnixMs"] = new Date().getTime();
        }
        if (dataBag.sortID) {
            params.Item![this.config.sortKey!] = dataBag.sortID;
        }
        return this.tryAcquireLock(params, dataBag, callback);
    }

    private acquireExistingLock(
        dataBag: AcquireLockData,
        lock: LockData,
        callback: (error: Error | undefined, lock?: Lock) => void,
    ) {
        const params: PutCommandInput = {
            TableName: this.config.lockTable,
            Item: {
                [this.config.partitionKey]: dataBag.partitionID,
                leaseDuration: getMsForLease(this.config.leaseDuration || 10000, this.config.leaseUnit).toString(),
                ownerName: dataBag.ownerName,
                recordVersionNumber: dataBag.recordVersionNumber,
                createdAt: new Date().toISOString(),
            },
            ConditionExpression: `${buildAttributeNotExistsExpression(
                this.config.sortKey,
            )} or (recordVersionNumber = :recordVersionNumber)`,
            ExpressionAttributeNames: buildExpressionAttributeNames(this.config.partitionKey, this.config.sortKey),
            ExpressionAttributeValues: {
                ":recordVersionNumber": lock.recordVersionNumber,
            },
        };
        if (this.config.trustLocalTime) {
            params.Item!.lockAcquiredTimeUnixMs = new Date().getTime();
        }
        if (dataBag.sortID) {
            params.Item![this.config.sortKey!] = dataBag.sortID;
        }
        return this.tryAcquireLock(params, dataBag, callback);
    }

    private tryAcquireLock(
        params: PutCommandInput,
        dataBag: AcquireLockData,
        callback: (error: Error | undefined, lock?: Lock) => void,
    ) {
        this.config.dynamodb.send(new PutCommand(params), (error) => {
            if (error) {
                if (error.code === "ConditionalCheckFailedException") {
                    if (dataBag.retryCount > 0) {
                        dataBag.retryCount--;
                        return this.checkForExistingLock(dataBag, callback);
                    } else {
                        const err = new FailOpenError("Failed to acquire lock.", "FailedToAcquireLock", error);
                        return callback(err);
                    }
                }
                return callback(error);
            }
            return callback(
                undefined,
                new Lock({
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
                }),
            );
        });
    }
}

export class Lock extends events.EventEmitter {
    private config: LockData;
    private recordVersionNumber: string;
    private released: boolean;
    private refreshing?: boolean;
    private refreshCallback?: () => void;
    private heartbeatTimeout?: NodeJS.Timeout;

    constructor(config: LockData) {
        super();
        this.config = config;
        this.recordVersionNumber = this.config.recordVersionNumber;
        this.released = false;
        if (this.config.heartbeatPeriodMs) {
            this.heartbeatTimeout = setTimeout(this.refreshLock.bind(this), this.config.heartbeatPeriodMs);
        }
    }

    release(callback: (error: Error | undefined) => void) {
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
            const params: DeleteCommandInput = {
                TableName: this.config.lockTable,
                Key: {
                    [this.config.partitionKey]: this.config.partitionID,
                },
                ConditionExpression: `${buildAttributeExistsExpression(
                    this.config.sortKey,
                )} and recordVersionNumber = :recordVersionNumber`,
                ExpressionAttributeNames: buildExpressionAttributeNames(this.config.partitionKey, this.config.sortKey),
                ExpressionAttributeValues: {
                    ":recordVersionNumber": this.recordVersionNumber,
                },
            };
            if (this.config.sortKey) {
                params.Key![this.config.sortKey] = this.config.sortID;
            }
            this.config.dynamodb.send(new DeleteCommand(params), (error) => {
                if (error && error.code === "ConditionalCheckFailedException") {
                    console.warn("WARNING: Failed to release lock: ConditionalCheckFailedException.");
                }
                return callback(undefined);
            });
        } catch (e) {
            console.warn(`WARNING: Failed to release lock: ${e}`);
            callback(undefined);
        }
    }

    private refreshLock() {
        this.refreshing = true;
        const newGuid = crypto.randomUUID();
        const params: PutCommandInput = {
            TableName: this.config.lockTable,
            Item: {
                [this.config.partitionKey]: this.config.partitionID,
                leaseDuration: getMsForLease(this.config.leaseDuration || 10000, this.config.leaseUnit).toString(),
                ownerName: this.config.ownerName,
                recordVersionNumber: newGuid,
                createdAt: new Date().toISOString(),
            },
            ConditionExpression: `${buildAttributeExistsExpression(
                this.config.sortKey,
            )} and recordVersionNumber = :recordVersionNumber`,
            ExpressionAttributeNames: buildExpressionAttributeNames(this.config.partitionKey, this.config.sortKey),
            ExpressionAttributeValues: {
                ":recordVersionNumber": this.recordVersionNumber,
            },
        };
        if (this.config.trustLocalTime) {
            params.Item!.lockAcquiredTimeUnixMs = new Date().getTime();
        }
        if (this.config.sortKey) {
            params.Item![this.config.sortKey] = this.config.sortID;
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
