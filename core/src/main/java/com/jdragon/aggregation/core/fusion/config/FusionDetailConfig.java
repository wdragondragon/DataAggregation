package com.jdragon.aggregation.core.fusion.config;

import com.jdragon.aggregation.commons.util.Configuration;
import lombok.Data;
import org.apache.commons.lang.StringUtils;

import java.io.File;

/**
 * 融合详情记录配置
 */
@Data
public class FusionDetailConfig {

    private boolean enabled = false;                    // 是否启用详情记录
    private String savePath;                            // 保存路径（目录或完整文件路径）
    private double samplingRate = 1.0;                 // 采样率 0.0-1.0（默认100%）
    private int maxRecords = 10000;                    // 最大记录条数（防内存溢出）
    private boolean includeSourceData = true;          // 是否包含源数据快照
    private boolean includeFieldDetails = true;        // 是否包含字段级详情
    private boolean saveHtml = false;
    private String fileNamePattern = "fusion_details_{timestamp}.json"; // 文件名模式

    /**
     * 从Configuration解析FusionDetailConfig
     */
    public static FusionDetailConfig fromConfig(Configuration config) {
        FusionDetailConfig detailConfig = new FusionDetailConfig();

        if (config == null) {
            return detailConfig;
        }

        detailConfig.setEnabled(config.getBool("enabled", true));
        detailConfig.setSavePath(config.getString("savePath", "./fusion_details"));
        detailConfig.setSamplingRate(config.getDouble("samplingRate", 1.0));
        detailConfig.setMaxRecords(config.getInt("maxRecords", 10000));
        detailConfig.setIncludeSourceData(config.getBool("includeSourceData", false));
        detailConfig.setIncludeFieldDetails(config.getBool("includeFieldDetails", false));
        detailConfig.setSaveHtml(config.getBool("saveHtml", false));
        detailConfig.setFileNamePattern(config.getString("fileNamePattern", "fusion_details_{timestamp}.json"));

        return detailConfig;
    }

    /**
     * 验证配置有效性
     */
    public void validate() {
        if (!enabled) {
            return; // 未启用时跳过验证
        }

        // 验证采样率
        if (samplingRate < 0.0 || samplingRate > 1.0) {
            throw new IllegalArgumentException("采样率必须在0.0到1.0之间: " + samplingRate);
        }

        // 验证最大记录数
        if (maxRecords <= 0) {
            throw new IllegalArgumentException("最大记录数必须大于0: " + maxRecords);
        }

        // 验证保存路径
        if (StringUtils.isBlank(savePath)) {
            throw new IllegalArgumentException("启用详情记录时必须指定savePath");
        }

        // 如果savePath是目录，确保目录存在或可创建
        if (!savePath.endsWith(".json")) {
            File dir = new File(savePath);
            if (!dir.exists() && !dir.mkdirs()) {
                throw new IllegalArgumentException("无法创建目录: " + savePath);
            }
        }
    }

    /**
     * 判断savePath是否是目录（不是以.json结尾）
     */
    public boolean isSavePathDirectory() {
        return savePath != null && !savePath.endsWith(".json");
    }

    /**
     * 获取完整的文件路径
     * @param jobId 作业ID（可选）
     * @param timestamp 时间戳（可选）
     * @return 完整的文件路径
     */
    public String getFullFilePath(String jobId, long timestamp) {
        if (StringUtils.isBlank(savePath)) {
            return null;
        }

        if (!isSavePathDirectory()) {
            // 已经是完整文件路径，直接返回
            return savePath;
        }

        // 是目录，需要生成文件名
        String fileName = generateFileName(jobId, timestamp);
        return savePath + File.separator + fileName;
    }

    /**
     * 生成文件名（根据模式替换占位符）
     */
    private String generateFileName(String jobId, long timestamp) {
        String fileName = fileNamePattern;

        // 替换时间戳占位符
        if (fileName.contains("{timestamp}")) {
            fileName = fileName.replace("{timestamp}", String.valueOf(timestamp));
        }

        // 替换作业ID占位符
        if (jobId != null && fileName.contains("{jobId}")) {
            fileName = fileName.replace("{jobId}", jobId);
        }

        // 替换日期时间占位符（可选扩展）
        if (fileName.contains("{datetime}")) {
            fileName = fileName.replace("{datetime}", new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date()));
        }

        // 确保文件名以.json结尾
        if (!fileName.endsWith(".json")) {
            fileName += ".json";
        }

        return fileName;
    }
}