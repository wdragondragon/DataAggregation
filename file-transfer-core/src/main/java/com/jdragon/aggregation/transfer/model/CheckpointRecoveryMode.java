package com.jdragon.aggregation.transfer.model;

public enum CheckpointRecoveryMode {
    /** Resume versioned checkpoints directly; restart legacy or invalid checkpoints from zero. */
    FAST,

    /** Rebuild the checksum from the source prefix when a legacy checkpoint is encountered. */
    STRICT
}
