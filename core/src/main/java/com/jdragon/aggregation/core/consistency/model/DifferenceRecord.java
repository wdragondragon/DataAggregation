package com.jdragon.aggregation.core.consistency.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class DifferenceRecord {

    private String recordId; // 璁板綍ID锛屽敮涓€鏍囪瘑绗?

    private Map<String, Object> matchKeyValues; // 鍖归厤閿€煎

    private Map<String, Map<String, Object>> sourceValues; // 鍚勬暟鎹簮鐨勫瓧娈靛€?

    private Map<String, String> differences; // 宸紓鎻忚堪锛屽瓧娈靛悕->宸紓鎻忚堪

    private ResolutionResult resolutionResult; // 瑙ｅ喅缁撴灉

    private UpdatePlan updatePlan; // 鏇存柊瑙勫垝

    private String conflictType; // 鍐茬獊绫诲瀷

    private double discrepancyScore; // 宸紓鍒嗘暟锛?.0-1.0锛?

    private List<String> missingSources; // 缂哄け鏁版嵁鐨勬暟鎹簮鍒楄〃

    public DifferenceRecord() {
        this.sourceValues = new HashMap<>();
        this.differences = new HashMap<>();
        this.matchKeyValues = new HashMap<>();
        this.missingSources = new ArrayList<>();
    }

    public void addSourceValue(String sourceId, Map<String, Object> values) {
        sourceValues.put(sourceId, values);
    }

    public void addDifference(String field, String description) {
        differences.put(field, description);
    }
}
