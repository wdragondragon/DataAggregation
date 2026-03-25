package com.jdragon.aggregation.core.fusion.config;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

/**
 * 数据源配置
 * 定义参与融合的单个数据源的配置信息
 */
@Data
public class SourceConfig {

    private String sourceId;               // 数据源ID，唯一标识符
    private String sourceName;             // 数据源名称，用于显示
    private String pluginType;             // 插件类型：mysql5, mysql8, influxdbv1等
    private Configuration pluginConfig;    // 插件配置

    private String querySql; // 查询SQL语句
    private String tableName;

    private double weight = 1.0;           // 权重：用于加权平均计算，数值越大权重越高
    private int priority = 0;              // 优先级：用于优先级融合，数值越大优先级越高
    private double confidence = 1.0;       // 置信度：用于高可信度策略

    private boolean isPrimary = false;     // 是否为主数据源（驱动源）
    private Integer maxRecords;            // 最大记录数限制（可选）

    private String incrColumn;
    private String incrModel;
    private Column maxIncrValue;

    private Map<String, String> fieldMappings; // 字段映射关系（源字段->内部字段名）
    private Configuration extConfig;       // 额外的扩展配置

    /**
     * 从Configuration解析SourceConfig
     */
    public static SourceConfig fromConfig(Configuration config) {
        SourceConfig sourceConfig = new SourceConfig();

        sourceConfig.setSourceId(config.getString("id"));
        sourceConfig.setSourceName(config.getString("name", config.getString("id")));
        sourceConfig.setPluginType(config.getString("type"));
        sourceConfig.setPluginConfig(config.getConfiguration("config"));

        // 解析融合相关参数
        sourceConfig.setWeight(config.getDouble("weight", 1.0));
        sourceConfig.setPriority(config.getInt("priority", 0));
        sourceConfig.setConfidence(config.getDouble("confidence", 1.0));

        sourceConfig.setPrimary(config.getBool("isPrimary", false));

        if (config.get("maxRecords") != null) {
            sourceConfig.setMaxRecords(config.getInt("maxRecords"));
        }

        sourceConfig.setFieldMappings(config.getMap("fieldMappings", String.class));
        sourceConfig.setExtConfig(config.getConfiguration("extConfig"));
        sourceConfig.setQuerySql(config.getString("querySql"));

        if (StringUtils.isBlank(sourceConfig.getQuerySql())) {
            String tableName = config.getString("table");
            List<String> columns = config.getList("columns", String.class);
            sourceConfig.setIncrColumn(config.getString("incrColumn"));
            sourceConfig.setIncrModel(config.getString("incrModel", ">"));
            String querySql = String.format("select %s from %s", String.join(",", columns), tableName);
            if (StringUtils.isNotBlank(sourceConfig.getIncrColumn())) {
                String maxPk = config.get("pkValue", null);
                if (maxPk != null) {
                    sourceConfig.setMaxIncrValue(new StringColumn(maxPk));
                    querySql += String.format("where %s %s '%s'", sourceConfig.getIncrColumn(), sourceConfig.getIncrModel(), maxPk);
                }
            }
            sourceConfig.setQuerySql(querySql);
        }

        return sourceConfig;
    }

    /**
     * 获取数据源的显示名称
     */
    public String getDisplayName() {
        return sourceName != null ? sourceName : sourceId;
    }

    /**
     * 验证配置有效性
     */
    public void validate() {
        if (sourceId == null || sourceId.trim().isEmpty()) {
            throw new IllegalArgumentException("数据源ID不能为空");
        }

        if (pluginType == null || pluginType.trim().isEmpty()) {
            throw new IllegalArgumentException("数据源插件类型不能为空");
        }

        if (pluginConfig == null) {
            throw new IllegalArgumentException("数据源插件配置不能为空");
        }

        if (weight <= 0) {
            throw new IllegalArgumentException("权重必须大于0: " + weight);
        }

        if (confidence < 0 || confidence > 1) {
            throw new IllegalArgumentException("置信度必须在0到1之间: " + confidence);
        }
    }

    /**
     * 比较优先级（用于排序）
     * 数值越大优先级越高
     */
    public int compareByPriority(SourceConfig other) {
        return Integer.compare(other.priority, this.priority); // 降序排序
    }

    /**
     * 比较权重（用于排序）
     * 数值越大权重越高
     */
    public int compareByWeight(SourceConfig other) {
        return Double.compare(other.weight, this.weight); // 降序排序
    }

    /**
     * 比较置信度（用于排序）
     * 数值越大置信度越高
     */
    public int compareByConfidence(SourceConfig other) {
        return Double.compare(other.confidence, this.confidence); // 降序排序
    }
}