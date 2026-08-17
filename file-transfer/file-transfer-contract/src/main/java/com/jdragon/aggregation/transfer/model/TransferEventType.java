package com.jdragon.aggregation.transfer.model;

public enum TransferEventType {
    VALIDATION_SUCCEEDED,
    BROWSE_RESULT,
    PLAN_STARTED,
    ITEM_PLANNED,
    PLAN_COMPLETED,
    RUN_STARTED,
    ITEM_STATUS_CHANGED,
    ITEM_PROGRESS,
    ITEM_RETRYING,
    CHECKPOINT_SAVED,
    RUN_COMPLETED
}
