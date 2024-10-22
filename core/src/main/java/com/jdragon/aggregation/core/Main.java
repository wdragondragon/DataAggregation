package com.jdragon.aggregation.core;

import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;

public class Main {
    public static void main(String[] args) {
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.READER, "mysql8reader")) {
            AbstractJobPlugin mysql8 = classLoaderSwapper.loadPlugin(AbstractJobPlugin.class);
            mysql8.init();
        }

        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.READER, "mysql5reader")) {
            AbstractJobPlugin mysql5 = classLoaderSwapper.loadPlugin(AbstractJobPlugin.class);
            mysql5.init();
        }
    }
}