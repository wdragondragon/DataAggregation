package com.jdragon.aggregation.core.consistency.model;

import com.jdragon.aggregation.commons.util.Configuration;
import lombok.Data;

import java.util.Map;

@Data
public class DataSourceConfig {

    private String sourceId;

    private String sourceName;

    private String pluginName;

    private Configuration connectionConfig;

    private String querySql;

    private String tableName;

    private Double confidenceWeight = 1.0;

    private int priority = 0;

    private Integer maxRecords;

    private Map<String, String> fieldMappings;

    private Configuration extConfig;

    private Boolean updateTarget = false;

    public static DataSourceConfig fromConfig(Configuration config) {
        DataSourceConfig dsConfig = new DataSourceConfig();
        dsConfig.setSourceId(config.getString("sourceId", config.getString("id")));
        dsConfig.setSourceName(config.getString("sourceName", config.getString("name", dsConfig.getSourceId())));
        dsConfig.setPluginName(config.getString("pluginName", config.getString("type")));
        dsConfig.setConnectionConfig(resolveConnectionConfig(config));
        dsConfig.setQuerySql(config.getString("querySql", config.getString("selectSql")));
        dsConfig.setTableName(config.getString("table", config.getString("tableName")));
        dsConfig.setConfidenceWeight(config.getDouble("confidenceWeight", 1.0));
        dsConfig.setPriority(config.getInt("priority", 0));
        if (config.get("maxRecords") != null) {
            dsConfig.setMaxRecords(config.getInt("maxRecords"));
        }
        dsConfig.setFieldMappings(config.getMap("fieldMappings", String.class));
        dsConfig.setExtConfig(resolveExtConfig(config));
        dsConfig.setUpdateTarget(config.getBool("updateTarget", false));
        return dsConfig;
    }

    private static Configuration resolveConnectionConfig(Configuration config) {
        Configuration connectionConfig = config.getConfiguration("connectionConfig");
        if (connectionConfig == null) {
            connectionConfig = config.getConfiguration("config");
        }
        if (connectionConfig == null) {
            connectionConfig = config.getConfiguration("connect");
        }
        return connectionConfig;
    }

    private static Configuration resolveExtConfig(Configuration config) {
        Configuration extConfig = config.getConfiguration("extConfig");
        if (extConfig != null) {
            return extConfig;
        }
        if (config.getMap("extConfig") != null) {
            return Configuration.from(config.getMap("extConfig"));
        }
        return Configuration.newDefault();
    }
}
