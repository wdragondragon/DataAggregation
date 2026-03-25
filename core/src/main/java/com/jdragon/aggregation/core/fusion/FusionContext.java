package com.jdragon.aggregation.core.fusion;

import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import com.jdragon.aggregation.core.fusion.config.SourceConfig;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.core.fusion.detail.FusionDetailRecorder;
import com.jdragon.aggregation.core.plugin.spi.reporter.JobPointReporter;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 融合执行上下文
 * 包含融合过程中的上下文信息和状态
 */
@Data
public class FusionContext {

    /**
     * 错误处理模式
     */
    public enum ErrorHandlingMode {
        STRICT,     // 严格模式：立即抛出异常
        LENIENT,    // 宽松模式：记录错误并跳过
        IGNORE      // 忽略模式：静默跳过
    }

    private FusionConfig fusionConfig;           // 融合配置
    private Map<String, SourceConfig> sourceConfigs; // 数据源配置映射
    private ErrorHandlingMode globalErrorMode;   // 全局错误处理模式

    // 执行状态
    private int processedRecords = 0;            // 已处理记录数
    private int skippedRecords = 0;              // 跳过的记录数
    private int errorCount = 0;                  // 错误计数

    // 错误跟踪
    private String currentJoinKey;               // 当前处理的连接键
    private List<FieldError> fieldErrors = new ArrayList<>(); // 字段级错误列表
    private Map<String, Integer> errorTypeCounts = new HashMap<>(); // 错误类型统计
    private List<String> targetColumns = new ArrayList<>();

    // 融合详情记录
    private FusionDetailRecorder detailRecorder;

    // 性能监控
    private long startTime;                      // 开始时间
    private long lastLogTime;                    // 上次日志时间

    private Map<String, Column> sourceMaxIncrValues = new HashMap<>();
    private Map<String, String> sourceIncrColumn = new HashMap<>();

    private JobPointReporter jobPointReporter;

    /**
     * 构造函数
     */
    public FusionContext(FusionConfig fusionConfig) {
        this.fusionConfig = fusionConfig;
        this.globalErrorMode = convertErrorMode(fusionConfig.getErrorMode());
        this.startTime = System.currentTimeMillis();
        this.lastLogTime = startTime;
        // 构建数据源配置映射
        this.sourceConfigs = new java.util.HashMap<>();
        for (com.jdragon.aggregation.core.fusion.config.SourceConfig source : fusionConfig.getSources()) {
            this.sourceConfigs.put(source.getSourceId(), source);
        }

        // 初始化融合详情记录器
        this.detailRecorder = new FusionDetailRecorder(fusionConfig);
    }

    public void updateIncrValue() {
        for (Map.Entry<String, SourceConfig> entry : this.sourceConfigs.entrySet()) {
            String sourceId = entry.getKey();
            SourceConfig source = entry.getValue();
            if (StringUtils.isNotBlank(source.getIncrColumn())) {
                sourceIncrColumn.put(sourceId, source.getIncrColumn());
                if (source.getMaxIncrValue() != null) {
                    sourceMaxIncrValues.put(sourceId, source.getMaxIncrValue());
                }
            }
        }
    }

    public void updateIncrValue(String sourceId, Column incrValue) {
        if (sourceIncrColumn.containsKey(sourceId)) {
            // 更新增量值
            Column maxPkValue = sourceMaxIncrValues.get(sourceId);
            if (maxPkValue == null || maxPkValue.compareTo(incrValue) < 0) {
//                maxPkValue = incrValue;
//                this.getJobPointReporter().put("pkValue_" + sourceId, maxPkValue.asString());
                sourceMaxIncrValues.put(sourceId, incrValue);
            }
        }
    }

    /**
     * 转换错误处理模式
     */
    private ErrorHandlingMode convertErrorMode(FusionConfig.ErrorHandlingMode configMode) {
        switch (configMode) {
            case STRICT:
                return ErrorHandlingMode.STRICT;
            case LENIENT:
                return ErrorHandlingMode.LENIENT;
            case MIXED:
                return ErrorHandlingMode.LENIENT; // 混合模式默认使用宽松
            default:
                return ErrorHandlingMode.LENIENT;
        }
    }

    /**
     * 获取字段级的错误处理模式
     */
    public ErrorHandlingMode getFieldErrorMode(String fieldName) {
        Map<String, FusionConfig.ErrorHandlingMode> fieldErrorModes = fusionConfig.getFieldErrorModes();
        if (fieldErrorModes != null && fieldErrorModes.containsKey(fieldName)) {
            return convertErrorMode(fieldErrorModes.get(fieldName));
        }
        return globalErrorMode;
    }

    /**
     * 处理字段映射错误
     */
    public Column handleFieldError(String fieldName, String errorMessage, Exception exception) {
        return handleFieldErrorWithJoinKey(currentJoinKey, fieldName, errorMessage, exception);
    }

    /**
     * 记录处理进度
     */
    public void recordProcessed() {
        processedRecords++;

        // 每处理1000条记录输出一次进度
        if (processedRecords % 1000 == 0) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastLogTime > 5000) { // 至少5秒输出一次
                double recordsPerSecond = 1000.0 * processedRecords / (currentTime - startTime);
                System.out.println(String.format("融合进度: %d 条记录, 速度: %.2f 条/秒, 错误: %d",
                        processedRecords, recordsPerSecond, errorCount));
                lastLogTime = currentTime;
            }
        }
    }

    /**
     * 记录跳过的记录
     */
    public void recordSkipped() {
        skippedRecords++;
    }

    /**
     * 获取处理统计信息
     */
    public String getStatistics() {
        long endTime = System.currentTimeMillis();
        double totalSeconds = (endTime - startTime) / 1000.0;
        double recordsPerSecond = totalSeconds > 0 ? processedRecords / totalSeconds : 0;

        return String.format(
                "融合完成统计:\n" +
                        "  总记录数: %d\n" +
                        "  跳过记录: %d\n" +
                        "  错误数量: %d\n" +
                        "  总时间: %.2f 秒\n" +
                        "  处理速度: %.2f 条/秒\n" +
                        "  成功率: %.2f%%",
                processedRecords, skippedRecords, errorCount, totalSeconds, recordsPerSecond,
                processedRecords > 0 ? 100.0 * (processedRecords - skippedRecords - errorCount) / processedRecords : 0
        );
    }

    /**
     * 获取数据源配置
     */
    public SourceConfig getSourceConfig(String sourceId) {
        return sourceConfigs != null ? sourceConfigs.get(sourceId) : null;
    }

    /**
     * 获取数据源权重
     */
    public double getSourceWeight(String sourceId) {
        SourceConfig config = getSourceConfig(sourceId);
        return config != null ? config.getWeight() : 1.0;
    }

    /**
     * 获取数据源优先级
     */
    public int getSourcePriority(String sourceId) {
        SourceConfig config = getSourceConfig(sourceId);
        return config != null ? config.getPriority() : 0;
    }

    /**
     * 获取数据源置信度
     */
    public double getSourceConfidence(String sourceId) {
        SourceConfig config = getSourceConfig(sourceId);
        return config != null ? config.getConfidence() : 1.0;
    }

    /**
     * 设置当前处理的连接键
     */
    public void setCurrentJoinKey(String joinKey) {
        this.currentJoinKey = joinKey;
    }

    /**
     * 获取当前连接键
     */
    public String getCurrentJoinKey() {
        return currentJoinKey;
    }

    /**
     * 处理字段映射错误（带连接键信息）
     * @param joinKey 连接键值
     * @param targetField 目标字段名
     * @param errorMessage 错误消息
     * @param exception 异常对象（可选）
     * @return 错误列（通常为null）
     */
    public Column handleFieldErrorWithJoinKey(String joinKey, String targetField, String errorMessage, Exception exception) {
        ErrorHandlingMode mode = getFieldErrorMode(targetField);

        // 记录错误详情
        FieldError error = new FieldError(joinKey, targetField, errorMessage, exception, mode);
        fieldErrors.add(error);

        // 更新错误类型统计
        String errorType = exception != null ? exception.getClass().getSimpleName() : "CONFIG_ERROR";
        errorTypeCounts.put(errorType, errorTypeCounts.getOrDefault(errorType, 0) + 1);

        switch (mode) {
            case STRICT:
                if (exception != null) {
                    throw new RuntimeException("字段映射错误 [joinKey=" + joinKey + ", targetField=" + targetField + "]: " + errorMessage, exception);
                } else {
                    throw new RuntimeException("字段映射错误 [joinKey=" + joinKey + ", targetField=" + targetField + "]: " + errorMessage);
                }
            case LENIENT:
                errorCount++;
                // 记录日志
                System.err.println("警告: 字段映射错误 [joinKey=" + joinKey + ", targetField=" + targetField + "]: " + errorMessage);
                if (exception != null) {
                    exception.printStackTrace();
                }
                return null; // 返回null表示跳过
            case IGNORE:
                errorCount++;
                return null; // 静默返回null
            default:
                return null;
        }
    }

    /**
     * 获取字段错误列表
     */
    public List<FieldError> getFieldErrors() {
        return new ArrayList<>(fieldErrors);
    }

    /**
     * 获取错误类型统计
     */
    public Map<String, Integer> getErrorTypeCounts() {
        return new HashMap<>(errorTypeCounts);
    }

    /**
     * 获取详细的错误统计信息
     */
    public String getErrorStatistics() {
        StringBuilder sb = new StringBuilder();
        sb.append("错误统计:\n");
        sb.append("  总错误数: ").append(errorCount).append("\n");
        sb.append("  错误类型分布:\n");
        for (Map.Entry<String, Integer> entry : errorTypeCounts.entrySet()) {
            sb.append("    ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }
        sb.append("  字段错误详情（前10条）:\n");
        int limit = Math.min(fieldErrors.size(), 10);
        for (int i = 0; i < limit; i++) {
            FieldError error = fieldErrors.get(i);
            sb.append("    [").append(i + 1).append("] ")
                    .append("joinKey=").append(error.getJoinKey())
                    .append(", targetField=").append(error.getTargetField())
                    .append(", error=").append(error.getErrorMessage())
                    .append(", mode=").append(error.getErrorMode())
                    .append("\n");
        }
        if (fieldErrors.size() > 10) {
            sb.append("    ... 还有 ").append(fieldErrors.size() - 10).append(" 条错误未显示\n");
        }
        return sb.toString();
    }

    /**
     * 字段错误信息类
     */
    @Data
    public static class FieldError {
        private final String joinKey;
        private final String targetField;
        private final String errorMessage;
        private final Exception exception;
        private final ErrorHandlingMode errorMode;
        private final long timestamp;

        public FieldError(String joinKey, String targetField, String errorMessage, Exception exception, ErrorHandlingMode errorMode) {
            this.joinKey = joinKey;
            this.targetField = targetField;
            this.errorMessage = errorMessage;
            this.exception = exception;
            this.errorMode = errorMode;
            this.timestamp = System.currentTimeMillis();
        }
    }

    // ========== 融合详情记录方法 ==========

    /**
     * 判断是否应该记录融合详情
     */
    public boolean shouldRecordFusionDetail() {
        return detailRecorder != null && detailRecorder.shouldRecord();
    }

    /**
     * 记录融合详情
     */
    public void recordFusionDetail(com.jdragon.aggregation.core.fusion.detail.FusionDetail detail) {
        if (detailRecorder != null) {
            detailRecorder.recordDetail(detail);
        }
    }

    /**
     * 保存融合详情到文件
     */
    public String saveFusionDetails() {
        if (detailRecorder != null) {
            return detailRecorder.saveToFile();
        }
        return null;
    }

    /**
     * 获取融合详情记录器
     */
    public FusionDetailRecorder getDetailRecorder() {
        return detailRecorder;
    }
}