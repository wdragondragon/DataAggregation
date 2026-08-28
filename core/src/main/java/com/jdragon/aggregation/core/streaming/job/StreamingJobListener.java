package com.jdragon.aggregation.core.streaming.job;

import com.jdragon.aggregation.core.statistics.communication.Communication;

public interface StreamingJobListener {

    StreamingJobListener NOOP = new StreamingJobListener() {
    };

    default void onBatchStarted(StreamingBatch batch) {
    }

    default void onBatchRetry(StreamingBatch batch, int attempt, Throwable failure) {
    }

    default void onBatchCommitted(StreamingBatch batch, Communication communication, long durationMillis) {
    }

    default void onBatchFailed(StreamingBatch batch, Throwable failure) {
    }
}
