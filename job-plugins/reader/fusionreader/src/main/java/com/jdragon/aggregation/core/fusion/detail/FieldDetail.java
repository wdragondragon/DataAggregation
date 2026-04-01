package com.jdragon.aggregation.core.fusion.detail;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * 字段级融合详情
 */
@Data
public class FieldDetail {
    
    private String targetField;                // 目标字段名
    private String sourceRef;                  // 源字段引用（${sourceId.fieldName}或${fieldName}）
    private Map<String, Object> sourceValues = new HashMap<>(); // 各数据源原始值 {sourceId: value}
    private Object fusedValue;                 // 融合后的最终值
    private String strategyUsed;               // 使用的融合策略
    private String mappingType;                // DIRECT, EXPRESSION, CONDITIONAL, GROOVY, CONSTANT
    private String status;                     // SUCCESS, ERROR, SKIPPED
    private String errorMessage;               // 错误信息
    
    /**
     * 添加源值
     */
    public void addSourceValue(String sourceId, Object value) {
        sourceValues.put(sourceId, value);
    }
    
    /**
     * 判断是否成功
     */
    public boolean isSuccess() {
        return "SUCCESS".equals(status);
    }
    
    /**
     * 判断是否错误
     */
    public boolean isError() {
        return "ERROR".equals(status);
    }
    
    /**
     * 判断是否跳过
     */
    public boolean isSkipped() {
        return "SKIPPED".equals(status);
    }
}