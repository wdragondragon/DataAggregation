package com.jdragon.aggregation.core.streaming;

public class SpillLimitExceededException extends RuntimeException {

    public SpillLimitExceededException(String message) {
        super(message);
    }

    public SpillLimitExceededException(String message, Throwable cause) {
        super(message, cause);
    }
}
