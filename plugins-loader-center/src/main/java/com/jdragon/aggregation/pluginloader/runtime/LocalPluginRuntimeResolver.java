package com.jdragon.aggregation.pluginloader.runtime;

import com.jdragon.aggregation.pluginloader.constant.SystemConstants;
import com.jdragon.aggregation.pluginloader.type.IPluginType;

import java.nio.file.Path;

/** Default resolver used by standalone DataAggregation distributions. */
public final class LocalPluginRuntimeResolver implements PluginRuntimeResolver {
    @Override
    public ResolvedPlugin resolve(IPluginType pluginType, String pluginName) {
        Path directory = Path.of(SystemConstants.PLUGIN_HOME, pluginType.getName(), pluginName)
                .toAbsolutePath().normalize();
        return new ResolvedPlugin(pluginType, pluginName, directory, "local:" + directory);
    }
}
