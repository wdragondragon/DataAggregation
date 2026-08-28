package com.jdragon.aggregation.core.streaming.job;

public interface StreamingSession extends AutoCloseable {

    StreamingBatch pollBatch(StreamingPollRequest request) throws Exception;

    void commit(StreamingCheckpoint checkpoint) throws Exception;

    default void abort(StreamingCheckpoint checkpoint, Throwable failure) {
    }

    default void wakeup() {
    }

    default StreamingMetricsSnapshot metrics() {
        return StreamingMetricsSnapshot.empty();
    }

    @Override
    void close();
}
