package com.jdragon.aggregation.plugin.httpdyn.exception;

public class HttpRequestConfigurationException extends HttpException {
    public HttpRequestConfigurationException(String message) {
        super(message);
    }

    public HttpRequestConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
