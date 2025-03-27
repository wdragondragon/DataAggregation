package com.jdragon.aggregation.httpreader;

import lombok.Getter;

@Getter
public enum ReadResultTypeEnum {
    /**
     * json
     */
    JSON("json"),

    /**
     * xml
     */
    XML("xml"),

    /**
     * soap
     */
    SOAP("soap");

    /**
     * 枚举值
     */
    private final String value;

    ReadResultTypeEnum(String value) {
        this.value = value;
    }
}
