package com.jdragon.aggregation.core.consistency.service;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.pluginloader.LoadUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class DataSourcePluginManager {

    private static final Map<String, AbstractDataSourcePlugin> pluginCache = new ConcurrentHashMap<>();

    public AbstractDataSourcePlugin getDataSourcePlugin(String pluginName) {
        return pluginCache.computeIfAbsent(pluginName, this::loadPlugin);
    }

    private AbstractDataSourcePlugin loadPlugin(String pluginName) {
        try {
            log.info("Loading data source plugin: {}", pluginName);
            AbstractDataSourcePlugin plugin = LoadUtil.loadJobPlugin(SourcePluginType.SOURCE, pluginName);
            plugin.init();
            log.info("Data source plugin loaded successfully: {}", pluginName);
            return plugin;
        } catch (Exception e) {
            log.error("Failed to load data source plugin: {}", pluginName, e);
            throw new RuntimeException("Failed to load data source plugin: " + pluginName, e);
        }
    }

    public void unloadPlugin(String pluginName) {
        AbstractDataSourcePlugin plugin = pluginCache.remove(pluginName);
        if (plugin != null) {
            try {
                plugin.destroy();
                log.info("Data source plugin unloaded: {}", pluginName);
            } catch (Exception e) {
                log.warn("Error while destroying plugin: {}", pluginName, e);
            }
        }
    }

    public void clearCache() {
        pluginCache.forEach((name, plugin) -> {
            try {
                plugin.destroy();
            } catch (Exception e) {
                log.warn("Error while destroying plugin: {}", name, e);
            }
        });
        pluginCache.clear();
        log.info("Cleared all data source plugins cache");
    }

    public static BaseDataSourceDTO createDataSourceDTO(com.jdragon.aggregation.core.consistency.model.DataSourceConfig config) {
        BaseDataSourceDTO dto = new BaseDataSourceDTO();
        dto.setName(config.getSourceName());

        if (config.getConnectionConfig() != null) {
            Configuration connectionConfig = config.getConnectionConfig();
            dto.setHost(connectionConfig.getString("host"));
            dto.setPort(connectionConfig.getString("port"));
            dto.setDatabase(connectionConfig.getString("database"));
            dto.setUserName(connectionConfig.getString("username", connectionConfig.getString("userName")));
            dto.setPassword(connectionConfig.getString("password"));
            dto.setUsePool(connectionConfig.getBool("usePool", false));
            dto.setJdbcUrl(connectionConfig.getString("jdbcUrl"));
            dto.setDriverClassName(connectionConfig.getString("driverClassName"));

            Map<String, String> extraParams = new LinkedHashMap<>();
            Map<String, Object> extraParamConfig = connectionConfig.getMap("extraParams");
            if (extraParamConfig != null) {
                for (Map.Entry<String, Object> entry : extraParamConfig.entrySet()) {
                    if (entry.getValue() != null) {
                        extraParams.put(entry.getKey(), String.valueOf(entry.getValue()));
                    }
                }
            }

            mergeStringValues(extraParams, config.getExtConfig());
            dto.setExtraParams(extraParams);

            Object other = connectionConfig.get("other");
            if (other instanceof Map) {
                dto.setOther(JSONObject.toJSONString(other));
            } else {
                String otherValue = connectionConfig.getString("other");
                if (StringUtils.isNotBlank(otherValue)) {
                    dto.setOther(otherValue);
                }
            }

            String pluginName = config.getPluginName();
            if (pluginName != null) {
                dto.setType(mapPluginNameToType(pluginName));
            }
        }

        return dto;
    }

    private static void mergeStringValues(Map<String, String> target, Configuration configuration) {
        if (configuration == null) {
            return;
        }
        Map<String, Object> values = configuration.getMap("");
        if (values == null) {
            return;
        }
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            if (entry.getValue() != null && !target.containsKey(entry.getKey())) {
                target.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
    }

    private static String mapPluginNameToType(String pluginName) {
        if (pluginName.contains("mysql5")) {
            return "mysql";
        } else if (pluginName.contains("mysql8")) {
            return "mysql8";
        } else if (pluginName.contains("mysql")) {
            return "mysql";
        } else if (pluginName.contains("postgresql") || pluginName.contains("postgres")) {
            return "postgresql";
        } else if (pluginName.contains("oracle")) {
            return "oracle";
        } else if (pluginName.contains("sqlserver")) {
            return "sqlserver";
        } else if (pluginName.contains("dm")) {
            return "dm";
        } else if (pluginName.contains("hive")) {
            return "hive";
        } else if (pluginName.contains("clickhouse")) {
            return "clickhouse";
        } else {
            return pluginName;
        }
    }
}
