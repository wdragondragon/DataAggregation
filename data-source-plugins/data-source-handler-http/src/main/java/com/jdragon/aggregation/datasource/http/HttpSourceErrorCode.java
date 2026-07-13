package com.jdragon.aggregation.datasource.http;

import com.jdragon.aggregation.commons.spi.ErrorCode;

public enum HttpSourceErrorCode implements ErrorCode {
    REQUIRED_VALUE("HttpSource-03", "HTTP source required value is missing"),
    RESPONSE_STATUS_ERROR("HttpSource-06", "HTTP response business status is not successful"),
    RESPONSE_PAGE_ERROR("HttpSource-08", "HTTP response page total path is invalid"),
    RESULT_TO_JSON_ERROR("HttpSource-09", "HTTP response cannot be converted to JSON");

    private final String code;
    private final String description;

    HttpSourceErrorCode(String code, String description) {
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
}
