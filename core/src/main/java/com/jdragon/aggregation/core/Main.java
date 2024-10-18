package com.jdragon.aggregation.core;

import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.pluginloader.ClassLoaderSwapper;
import com.jdragon.aggregation.pluginloader.LoadUtil;

import java.sql.SQLException;

public class Main {
    public static void main(String[] args) {
        ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper.newCurrentThreadClassLoaderSwapper();

        classLoaderSwapper.setCurrentThreadClassLoader(PluginType.READER, "mysql8reader");
        AbstractJobPlugin mysql8 = LoadUtil.loadJobPlugin(PluginType.READER, "mysql8reader", AbstractJobPlugin.class);
        mysql8.init();
        classLoaderSwapper.restoreCurrentThreadClassLoader();


        classLoaderSwapper.setCurrentThreadClassLoader(PluginType.READER, "mysql5reader");
        AbstractJobPlugin mysql5 = LoadUtil.loadJobPlugin(PluginType.READER, "mysql5reader", AbstractJobPlugin.class);
        mysql5.init();
        classLoaderSwapper.restoreCurrentThreadClassLoader();
    }
}