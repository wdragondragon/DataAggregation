package com.jdragon.aggregation.unstructuredstorage;

import lombok.Data;

@Data
public class ColumnEntry {
    private Integer index;

    private String type;

    private String value;

    private String format;

    private String name;

    private String parentNode;
}
