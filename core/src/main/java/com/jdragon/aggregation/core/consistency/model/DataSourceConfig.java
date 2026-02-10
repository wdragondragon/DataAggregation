package com.jdragon.aggregation.core.consistency.model;

import com.jdragon.aggregation.commons.util.Configuration;
import lombok.Data;

import java.util.Map;

@Data
public class DataSourceConfig {

    private String sourceId; // 数据源ID，唯一标识符

    private String sourceName; // 数据源名称，用于显示

    private String pluginName; // 数据源插件名称

    private Configuration connectionConfig; // 连接配置

    private String querySql; // 查询SQL语句

    private String tableName; // 表名（如果使用表模式）

    private Double confidenceWeight = 1.0; // 置信度权重，用于冲突解决

    private int priority = 0; // 优先级，数值越小优先级越高

    private Integer maxRecords; // 最大记录数限制

    private Map<String, String> fieldMappings; // 字段映射关系

    private Boolean updateTarget = false; // 是否为更新目标数据源

    public static DataSourceConfig fromConfig(Configuration config) {
        DataSourceConfig dsConfig = new DataSourceConfig();
        dsConfig.setSourceId(config.getString("sourceId"));
        dsConfig.setSourceName(config.getString("sourceName"));
        dsConfig.setPluginName(config.getString("pluginName"));
        dsConfig.setConnectionConfig(config.getConfiguration("connectionConfig"));
        dsConfig.setQuerySql(config.getString("querySql"));
        dsConfig.setTableName(config.getString("tableName"));
        dsConfig.setConfidenceWeight(config.getDouble("confidenceWeight", 1.0));
        dsConfig.setPriority(config.getInt("priority", 0));
        if (config.get("maxRecords") != null) {
            dsConfig.setMaxRecords(config.getInt("maxRecords"));
        }
        dsConfig.setFieldMappings(config.getMap("fieldMappings", String.class));
        dsConfig.setUpdateTarget(config.getBool("updateTarget", false));
        return dsConfig;
    }
}