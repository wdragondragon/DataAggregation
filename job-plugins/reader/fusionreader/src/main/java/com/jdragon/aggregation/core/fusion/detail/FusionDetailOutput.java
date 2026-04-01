package com.jdragon.aggregation.core.fusion.detail;

import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import com.jdragon.aggregation.core.fusion.config.FusionDetailConfig;
import com.jdragon.aggregation.core.fusion.config.SourceConfig;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 融合详情输出（最终JSON结构）
 */
@Data
public class FusionDetailOutput {
    
    private Metadata metadata;                 // 元数据
    private List<FusionDetail> fusionDetails = new ArrayList<>(); // 融合详情列表
    private Summary summary;                   // 统计摘要
    
    @Data
    public static class Metadata {
        private String fusionJobId;            // 融合作业ID
        private long timestamp;                // 生成时间戳
        private FusionConfig config;           // 融合配置
        private List<SourceConfig> dataSources; // 数据源配置
        private List<Map<String, Object>> fieldMappings; // 字段映射配置（简化）
        private FusionDetailConfig detailConfig; // 详情记录配置
        
        public Metadata() {
            this.timestamp = System.currentTimeMillis();
        }
    }
    
    @Data
    public static class Summary {
        private int totalRecords;              // 总记录数
        private int sampledRecords;            // 采样记录数
        private int successfulRecords;         // 成功记录数
        private int errorRecords;              // 错误记录数
        private int skippedRecords;            // 跳过记录数
        private long processingTimeMs;         // 处理时间（毫秒）
        private double samplingRate;           // 采样率
        private Map<String, Integer> fieldStatistics = new HashMap<>(); // 字段统计
        private Map<String, Integer> strategyStatistics = new HashMap<>(); // 策略使用统计
        private Map<String, Integer> mappingTypeStatistics = new HashMap<>(); // 映射类型统计
        
        /**
         * 更新统计信息
         */
        public void update(FusionDetail detail) {
            sampledRecords++;
            
            if ("SUCCESS".equals(detail.getStatus())) {
                successfulRecords++;
            } else if ("ERROR".equals(detail.getStatus())) {
                errorRecords++;
            } else if ("SKIPPED".equals(detail.getStatus())) {
                skippedRecords++;
            }
            
            // 更新字段统计
            for (FieldDetail fieldDetail : detail.getFieldDetails()) {
                String targetField = fieldDetail.getTargetField();
                fieldStatistics.put(targetField, fieldStatistics.getOrDefault(targetField, 0) + 1);
                
                String strategy = fieldDetail.getStrategyUsed();
                if (strategy != null) {
                    strategyStatistics.put(strategy, strategyStatistics.getOrDefault(strategy, 0) + 1);
                }
                
                String mappingType = fieldDetail.getMappingType();
                if (mappingType != null) {
                    mappingTypeStatistics.put(mappingType, mappingTypeStatistics.getOrDefault(mappingType, 0) + 1);
                }
            }
        }
        
        /**
         * 计算成功率
         */
        public double getSuccessRate() {
            return sampledRecords > 0 ? (double) successfulRecords / sampledRecords * 100 : 0.0;
        }
        
        /**
         * 计算错误率
         */
        public double getErrorRate() {
            return sampledRecords > 0 ? (double) errorRecords / sampledRecords * 100 : 0.0;
        }
    }
    
    /**
     * 创建默认输出
     */
    public static FusionDetailOutput createDefault() {
        FusionDetailOutput output = new FusionDetailOutput();
        output.setMetadata(new Metadata());
        output.setSummary(new Summary());
        return output;
    }
}