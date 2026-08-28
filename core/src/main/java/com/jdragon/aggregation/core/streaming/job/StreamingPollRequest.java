package com.jdragon.aggregation.core.streaming.job;

public final class StreamingPollRequest {

    public static final long DEFAULT_TIMEOUT_MILLIS = 1_000L;
    public static final int DEFAULT_MAX_RECORDS = 1_000;
    public static final long DEFAULT_MAX_BYTES = 16L * 1024L * 1024L;

    private final long timeoutMillis;
    private final int maxRecords;
    private final long maxBytes;

    public StreamingPollRequest(long timeoutMillis, int maxRecords, long maxBytes) {
        this.timeoutMillis = Math.max(1L, timeoutMillis);
        this.maxRecords = Math.max(1, maxRecords);
        this.maxBytes = Math.max(1L, maxBytes);
    }

    public static StreamingPollRequest defaults() {
        return new StreamingPollRequest(DEFAULT_TIMEOUT_MILLIS, DEFAULT_MAX_RECORDS, DEFAULT_MAX_BYTES);
    }

    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    public int getMaxRecords() {
        return maxRecords;
    }

    public long getMaxBytes() {
        return maxBytes;
    }
}
