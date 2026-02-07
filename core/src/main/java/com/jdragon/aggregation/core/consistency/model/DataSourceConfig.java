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

    private Map<String, String> fieldMappings;

    private Boolean updateTarget = false;

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
        dsConfig.setFieldMappings(config.getMap("fieldMappings", String.class));
        dsConfig.setUpdateTarget(config.getBool("updateTarget", false));
        return dsConfig;
    }
}