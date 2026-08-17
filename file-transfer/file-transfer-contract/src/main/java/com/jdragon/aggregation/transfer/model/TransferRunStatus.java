package com.jdragon.aggregation.transfer.model;

public enum TransferRunStatus {
    QUEUED,
    RUNNING,
    PAUSED,
    SUCCESS,
    PARTIAL_SUCCESS,
    FAILED,
    CANCELED
}
