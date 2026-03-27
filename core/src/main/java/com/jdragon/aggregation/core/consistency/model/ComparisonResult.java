package com.jdragon.aggregation.core.consistency.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class ComparisonResult {

    private String resultId; // 结果ID，唯一标识符

    private String ruleId; // 规则ID

    private Date executionTime; // 执行时间

    private Status status; // 执行状态

    private long totalRecords; // 总记录数

    private long consistentRecords; // 一致记录数

    private long inconsistentRecords; // 不一致记录数

    private long resolvedRecords; // 已解决记录数

    private Map<String, Integer> fieldDiscrepancies; // 字段差异统计

    private Map<String, Object> summary; // 摘要信息

    private String reportPath; // 报告文件路径

    private Map<String, Object> metadata; // 元数据

    private List<Map<String, Object>> resolvedRows; // 已解决行数据

    private Map<String, Number> sourceDataCount; // 数据源数据量

    private UpdateResult updateResult; // 更新结果

    // 以下属性为后置填充
    private List<ResolutionResult> resolutionResults;

    private List<DifferenceRecord> differenceRecords;

    private String compareResultOutputAbsPath;

    private String differenceOutputAbsPath;

    private String resolutionOutputAbsPath;

    private String executionEngine;

    public ComparisonResult() {
        this.executionTime = new Date();
        this.fieldDiscrepancies = new HashMap<>();
        this.summary = new HashMap<>();
        this.metadata = new HashMap<>();
        this.resolvedRows = new ArrayList<>();
    }

    public void incrementConsistent() {
        consistentRecords++;
    }

    public void incrementInconsistent() {
        inconsistentRecords++;
    }

    public void incrementResolved() {
        resolvedRecords++;
    }

    public void addFieldDiscrepancy(String field, int count) {
        fieldDiscrepancies.put(field, fieldDiscrepancies.getOrDefault(field, 0) + count);
    }

    public enum Status {
        SUCCESS, // 完全成功
        PARTIAL_SUCCESS, // 部分成功（有差异但已解决）
        FAILED, // 失败
        RUNNING // 运行中
    }
}
