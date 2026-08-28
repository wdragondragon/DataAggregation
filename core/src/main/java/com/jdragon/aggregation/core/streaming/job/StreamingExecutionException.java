package com.jdragon.aggregation.core.streaming.job;

public class StreamingExecutionException extends RuntimeException {

    public StreamingExecutionException(String message) {
        super(message);
    }

    public StreamingExecutionException(String message, Throwable cause) {
        super(message, cause);
    }
}
