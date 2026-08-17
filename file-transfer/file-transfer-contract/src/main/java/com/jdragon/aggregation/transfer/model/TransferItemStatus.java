package com.jdragon.aggregation.transfer.model;

public enum TransferItemStatus {
    DISCOVERING,
    QUEUED,
    TRANSFERRING,
    VERIFYING,
    COMMITTING,
    POST_ACTION,
    SUCCESS,
    SKIPPED,
    CONFLICT,
    POST_ACTION_FAILED,
    FAILED,
    PAUSED,
    CANCELED;

    public boolean terminal() {
        return this == SUCCESS || this == SKIPPED || this == CONFLICT
                || this == POST_ACTION_FAILED || this == FAILED || this == CANCELED;
    }
}
