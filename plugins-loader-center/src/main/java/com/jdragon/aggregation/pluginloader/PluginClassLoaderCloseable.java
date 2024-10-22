package com.jdragon.aggregation.pluginloader;

import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import com.jdragon.aggregation.pluginloader.type.IPluginType;

public final class PluginClassLoaderCloseable implements AutoCloseable {

    private ClassLoaderSwapper classLoaderSwapper;

    private IPluginType pluginType;

    private String pluginName;

    public static PluginClassLoaderCloseable newCurrentThreadClassLoaderSwapper(IPluginType pluginType, String pluginName) {
        PluginClassLoaderCloseable classLoaderSwapper = new PluginClassLoaderCloseable();
        classLoaderSwapper.pluginName = pluginName;
        classLoaderSwapper.pluginType = pluginType;
        classLoaderSwapper.classLoaderSwapper = ClassLoaderSwapper.newCurrentThreadClassLoaderSwapper(pluginType, pluginName);
        return classLoaderSwapper;
    }

    public <T extends AbstractPlugin> T loadPlugin(Class<T> tClass) {
        return LoadUtil.loadJobPlugin(pluginType, pluginName, tClass);
    }


    @Override
    public void close() {
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }
}