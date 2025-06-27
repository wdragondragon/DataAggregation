package com.jdragon.aggregation.writer.influxdbv1;

import com.jdragon.aggregation.commons.spi.ErrorCode;

public enum InfluxDBV1ErrorCode implements ErrorCode {

    INFLUX_WRITE_ERROR("Influxdbv1Writer-01", "写入失败");

    private final String code;
    private final String description;

    private InfluxDBV1ErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }
}
