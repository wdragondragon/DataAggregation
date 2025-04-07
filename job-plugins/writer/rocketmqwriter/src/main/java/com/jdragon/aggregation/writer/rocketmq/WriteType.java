package com.jdragon.aggregation.writer.rocketmq;

/**
 * @author js0014
 */

public enum WriteType {
    /**
     * json
     */
    JSON("json"),
    /**
     * rawData
     */
    RAWDATA("rawData"),
    /**
     * split
     */
    SPLIT("split");

    /**
     * 名称
     */
    private String name;

    WriteType(String name) {
        this.name = name;
    }
}
