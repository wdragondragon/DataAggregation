package com.jdragon.aggregation.plugin.httpdyn.exception;

/**
 * @Author: Jdragon
 * @email: 1061917196@qq.com
 * @Date: 2020.08.01 10:32
 * @Description:
 */
public class HttpException extends RuntimeException {
    public HttpException() {
    }

    public HttpException(String message) {
        super(message);
    }

    public HttpException(Throwable e) {
        super(e);
    }

    public HttpException(String message, Throwable cause) {
        super(message, cause);
    }
}
