package com.jdragon.aggregation.writer.http;

import com.jdragon.aggregation.commons.spi.ErrorCode;

public enum HttpWriterErrorCode implements ErrorCode {
    REQUIRED_VALUE("HttpWriter-00", "您缺失了必须填写的参数值!"),
    ILLEGAL_VALUE("HttpWriter-01", "您填写的参数值不合法!"),
    REQUEST_ERROR("HttpWriter-02", "HTTP 写入请求失败!"),
    RESPONSE_STATUS_ERROR("HttpWriter-03", "HTTP 写入接口返回业务状态码与成功状态码不一致!");

    private final String code;
    private final String description;

    HttpWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", code, description);
    }
}
