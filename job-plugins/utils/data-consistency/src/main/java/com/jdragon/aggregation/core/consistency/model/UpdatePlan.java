package com.jdragon.aggregation.core.consistency.model;

import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class UpdatePlan {

    private String operationType;

    private String targetSourceId;

    private String referenceSourceId;

    private Map<String, Object> resolvedValues = new LinkedHashMap<>();

    private Map<String, Object> matchKeyValues = new LinkedHashMap<>();

    private String reason;
}
