package com.jdragon.aggregation.core.consistency.model;

import lombok.Data;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class UpdateResult {

    private String resultId; // 结果ID，唯一标识符

    private String ruleId; // 规则ID

    private Date executionTime; // 执行时间

    private String targetSourceId; // 目标数据源ID

    private int totalUpdates; // 总更新操作数

    private int successfulUpdates; // 成功更新数

    private int failedUpdates; // 失败更新数

    private int insertCount; // 插入操作数
    
    private int updateCount; // 更新操作数
    
    private int deleteCount; // 删除操作数
    
    private int skipCount; // 跳过操作数
    
    private Map<String, String> operationTypes; // 记录ID -> 操作类型映射

    private List<UpdateFailure> failures; // 失败详情列表

    private Map<String, Object> summary; // 摘要信息

    public UpdateResult() {
        this.executionTime = new Date();
        this.failures = new ArrayList<>();
        this.successfulUpdates = 0;
        this.failedUpdates = 0;
        this.totalUpdates = 0;
        this.insertCount = 0;
        this.updateCount = 0;
        this.deleteCount = 0;
        this.skipCount = 0;
        this.operationTypes = new HashMap<>();
    }

    public void incrementSuccessful() {
        successfulUpdates++;
        totalUpdates++;
    }

    public void incrementFailed(String recordId, String reason) {
        failedUpdates++;
        totalUpdates++;
        failures.add(new UpdateFailure(recordId, reason));
    }

    public void incrementFailed(String recordId, String reason, Map<String, Object> matchKeys) {
        failedUpdates++;
        totalUpdates++;
        failures.add(new UpdateFailure(recordId, reason, matchKeys));
    }

    public void incrementInsert(String recordId) {
        insertCount++;
        operationTypes.put(recordId, "INSERT");
    }

    public void incrementUpdate(String recordId) {
        updateCount++;
        operationTypes.put(recordId, "UPDATE");
    }

    public void incrementDelete(String recordId) {
        deleteCount++;
        operationTypes.put(recordId, "DELETE");
    }
    
    public void incrementSkip(String recordId, String reason) {
        skipCount++;
        operationTypes.put(recordId, "SKIP");
    }

    public int getInsertCount() {
        return insertCount;
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public int getDeleteCount() {
        return deleteCount;
    }
    
    public int getSkipCount() {
        return skipCount;
    }

    public Map<String, String> getOperationTypes() {
        return operationTypes;
    }

    @Data
    public static class UpdateFailure {
        private String recordId; // 记录ID
        private Map<String, Object> matchKeys; // 匹配键值对
        private String reason; // 失败原因
        private Date failureTime; // 失败时间

        public UpdateFailure(String recordId, String reason) {
            this.recordId = recordId;
            this.reason = reason;
            this.failureTime = new Date();
        }

        public UpdateFailure(String recordId, String reason, Map<String, Object> matchKeys) {
            this.recordId = recordId;
            this.reason = reason;
            this.matchKeys = matchKeys;
            this.failureTime = new Date();
        }
    }
}