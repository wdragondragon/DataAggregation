package com.jdragon.aggregation.commons.exception;


import com.jdragon.aggregation.commons.spi.ErrorCode;

import java.io.PrintWriter;
import java.io.StringWriter;

public class AggregationException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private ErrorCode errorCode;

    public AggregationException(ErrorCode errorCode, String errorMessage) {
        super(errorCode.toString() + " - " + errorMessage);
        this.errorCode = errorCode;
    }

    public AggregationException(String errorMessage) {
        super(errorMessage);
    }


    private AggregationException(ErrorCode errorCode, String errorMessage, Throwable cause) {
        super(errorCode.toString() + " - " + getMessage(errorMessage) + " - " + getMessage(cause), cause);

        this.errorCode = errorCode;
    }

    public static AggregationException asException(ErrorCode errorCode, String message) {
        return new AggregationException(errorCode, message);
    }

    public static AggregationException asException(String message) {
        return new AggregationException(message);
    }

    public static AggregationException asException(ErrorCode errorCode, String message, Throwable cause) {
        if (cause instanceof AggregationException) {
            return (AggregationException) cause;
        }
        return new AggregationException(errorCode, message, cause);
    }

    public static AggregationException asException(ErrorCode errorCode, Throwable cause) {
        if (cause instanceof AggregationException) {
            return (AggregationException) cause;
        }
        return new AggregationException(errorCode, getMessage(cause), cause);
    }

    public ErrorCode getErrorCode() {
        return this.errorCode;
    }

    private static String getMessage(Object obj) {
        if (obj == null) {
            return "";
        }

        if (obj instanceof Throwable) {
            StringWriter str = new StringWriter();
            PrintWriter pw = new PrintWriter(str);
            ((Throwable) obj).printStackTrace(pw);
            return str.toString();
            // return ((Throwable) obj).getMessage();
        } else {
            return obj.toString();
        }
    }
}
