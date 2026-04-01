package com.jdragon.aggregation.core.fusion.detail;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 单行融合详情
 */
@Data
public class FusionDetail {
    
    private String joinKey;                    // 关联键值（行标识）
    private long timestamp;                    // 处理时间戳
    private Map<String, Map<String, Object>> sourceRows = new HashMap<>(); // 源数据快照 {sourceId: {field: value}}
    private List<FieldDetail> fieldDetails = new ArrayList<>(); // 字段级详情列表
    private String status;                     // SUCCESS, ERROR, SKIPPED
    private String errorMessage;               // 错误信息（如果有）
    
    /**
     * 添加源行数据
     */
    public void addSourceRow(String sourceId, Map<String, Object> rowData) {
        sourceRows.put(sourceId, rowData);
    }
    
    /**
     * 添加字段详情
     */
    public void addFieldDetail(FieldDetail fieldDetail) {
        fieldDetails.add(fieldDetail);
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
    
    /**
     * 获取字段详情数量
     */
    public int getFieldDetailCount() {
        return fieldDetails.size();
    }
    
    /**
     * 获取涉及的数据源ID列表
     */
    public List<String> getSourceIds() {
        return new ArrayList<>(sourceRows.keySet());
    }
}