package com.jdragon.aggregation.pluginloader;

import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import com.jdragon.aggregation.pluginloader.type.IPluginType;

public final class PluginClassLoaderCloseable implements AutoCloseable {

    private final ClassLoaderSwapper classLoaderSwapper;

    private final IPluginType pluginType;

    private final String pluginName;

    public static PluginClassLoaderCloseable newCurrentThreadClassLoaderSwapper(IPluginType pluginType, String pluginName) {
        return new PluginClassLoaderCloseable(pluginType, pluginName);
    }

    private PluginClassLoaderCloseable(IPluginType pluginType, String pluginName) {
        this.pluginName = pluginName;
        this.pluginType = pluginType;
        this.classLoaderSwapper = ClassLoaderSwapper.newCurrentThreadClassLoaderSwapper(pluginType, pluginName);
    }

    public <T extends AbstractPlugin> T loadPlugin() {
        return LoadUtil.loadJobPlugin(pluginType, pluginName);
    }


    @Override
    public void close() {
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }
}