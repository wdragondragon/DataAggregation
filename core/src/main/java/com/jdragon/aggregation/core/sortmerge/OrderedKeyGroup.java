package com.jdragon.aggregation.core.sortmerge;

import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class OrderedKeyGroup {
    private String sourceId;
    private OrderedKey key;
    private Map<String, Object> firstRow = new LinkedHashMap<String, Object>();
    private int duplicateCount;
    private long scannedRecords;
}
