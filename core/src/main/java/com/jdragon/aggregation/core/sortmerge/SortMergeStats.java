package com.jdragon.aggregation.core.sortmerge;

import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class SortMergeStats {
    private String executionEngine = "sortmerge";
    private long mergeResolvedKeyCount;
    private long mergeSpilledKeyCount;
    private long duplicateIgnoredCount;
    private String fallbackReason;
    private Map<String, Long> sourceRecordCounts = new LinkedHashMap<String, Long>();
    private Map<String, Long> sourceGroupCounts = new LinkedHashMap<String, Long>();
}
