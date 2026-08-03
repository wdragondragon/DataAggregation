package com.jdragon.aggregation.pluginloader;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.pluginloader.constant.SystemConstants;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeSession;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeResolvers;
import com.jdragon.aggregation.pluginloader.runtime.ResolvedPlugin;
import com.jdragon.aggregation.pluginloader.type.IPluginType;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;

public class ConfigParser {

    private static final Map<String, Configuration> pluginConfigMap = new ConcurrentHashMap<>();

    public static Configuration parseOnePluginConfig(IPluginType pluginType, String pluginName) {
        PluginRuntimeSession session = PluginRuntimeSession.current();
        ResolvedPlugin resolved = session == null
                ? PluginRuntimeResolvers.resolve(pluginType, pluginName)
                : session.resolve(pluginType, pluginName);
        return parseOnePluginConfig(resolved, pluginType);
    }

    public static Configuration parseOnePluginConfig(ResolvedPlugin resolvedPlugin, IPluginType pluginType) {
        return parseOnePluginConfig(resolvedPlugin.getDirectory().toString(), pluginType,
                resolvedPlugin.getRuntimeKey());
    }

    public static Configuration parsePluginConfig(List<IPluginType> wantPluginTypes) {
        Configuration configuration = Configuration.newDefault();
        for (IPluginType wantPluginType : wantPluginTypes) {
            String pluginHome = StringUtils.join(
                    new String[]{SystemConstants.PLUGIN_HOME, wantPluginType.getName()}, File.separator);
            for (final String each : ConfigParser.getDirAsList(pluginHome)) {
                Configuration eachReaderConfig = ConfigParser.parseOnePluginConfig(each, wantPluginType);
                configuration.merge(eachReaderConfig, true);
            }
        }
        return configuration;
    }


    public static Configuration parseOnePluginConfig(final String path,
                                                     final IPluginType type) {
        return parseOnePluginConfig(path, type, path);
    }

    private static Configuration parseOnePluginConfig(final String path,
                                                      final IPluginType type,
                                                      final String cacheNamespace) {
        String fileName = type.getName().equals("transformer") ? "transformer.json" : "plugin.json";
        String filePath = path + File.separator + fileName;
        String cacheKey = cacheNamespace + "|" + filePath;
        if (pluginConfigMap.containsKey(cacheKey)) {
            return pluginConfigMap.get(cacheKey);
        }

        Configuration configuration = Configuration.from(new File(filePath));

        String pluginPath = configuration.getString("path");
        String pluginName = configuration.getString("name");

        boolean isDefaultPath = StringUtils.isBlank(pluginPath);
        if (isDefaultPath) {
            configuration.set("path", path);
        }

        Configuration result = Configuration.newDefault();

        result.set(
                String.format("plugin.%s.%s", type.getName(), pluginName),
                configuration.getInternal());
        pluginConfigMap.put(cacheKey, result);
        return result;
    }

    public static void clearCache() {
        pluginConfigMap.clear();
    }

    private static List<String> getDirAsList(String path) {
        List<String> result = new ArrayList<>();

        String[] paths = new File(path).list();
        if (null == paths) {
            return result;
        }

        for (final String each : paths) {
            result.add(path + File.separator + each);
        }

        return result;
    }
}
