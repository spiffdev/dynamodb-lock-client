import * as events from 'events';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

type Excluding<Type, ReservedKeys> = (Type extends ReservedKeys ? never : Type) & (ReservedKeys extends Type ? never : Type);
type StringExcludingReservedKeys = Excluding<string, "leaseDuration" | "lockAcquiredTimeUnixMs" | "ownerName" | "recordVersionNumber">;
type LeaseUnit = "milliseconds" | "seconds" | "minutes" | "hours" | "days";
interface FailOpenConfig {
    ownerName: string;
    dynamodb: DynamoDBDocumentClient;
    lockTable: string;
    partitionKey: StringExcludingReservedKeys;
    sortKey?: StringExcludingReservedKeys;
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
declare class FailOpenError extends Error {
    code: string;
    originalError: Error;
    constructor(message: string, code: string, originalError: Error);
}
declare class FailOpen<PartitionTableKeyType extends string | number | Record<string, any>> {
    private config;
    private retryCount;
    constructor(config: FailOpenConfig);
    acquireLock(id: PartitionTableKeyType, callback: (error: Error | undefined, lock?: Lock) => void): void;
    private checkForExistingLock;
    private acquireNewLock;
    private acquireExistingLock;
    private tryAcquireLock;
}
declare class Lock extends events.EventEmitter {
    private config;
    private recordVersionNumber;
    private released;
    private refreshing?;
    private refreshCallback?;
    private heartbeatTimeout?;
    constructor(config: LockData);
    release(callback: (error: Error | undefined) => void): void;
    private refreshLock;
}

export { FailOpen, type FailOpenConfig, FailOpenError, Lock };
