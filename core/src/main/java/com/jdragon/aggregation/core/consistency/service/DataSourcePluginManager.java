package com.jdragon.aggregation.core.consistency.service;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.pluginloader.LoadUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

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

        if (config.getConnectionConfig() != null) {
            dto.setHost(config.getConnectionConfig().getString("host"));
            dto.setPort(config.getConnectionConfig().getString("port"));
            dto.setDatabase(config.getConnectionConfig().getString("database"));
            dto.setUserName(config.getConnectionConfig().getString("username"));
            dto.setPassword(config.getConnectionConfig().getString("password"));
            Map<String, String> other = config.getConnectionConfig().getMap("other", String.class);
            if (other != null) {
                dto.setOther(JSONObject.toJSONString(other));
            }

            String pluginName = config.getPluginName();
            if (pluginName != null) {
                dto.setType(mapPluginNameToType(pluginName));
            }
        }

        return dto;
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