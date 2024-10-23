package com.jdragon.aggregation.core;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;

import java.util.Map;

public class Main {
    public static void main(String[] args) {
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.READER, "mysql8reader")) {
            AbstractJobPlugin mysql8 = classLoaderSwapper.loadPlugin();
            mysql8.init();
        }

        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.READER, "mysql5reader")) {
            AbstractJobPlugin mysql5 = classLoaderSwapper.loadPlugin();
            mysql5.init();
        }


        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, "mysql5")) {
            AbstractDataSourcePlugin mysql5 = classLoaderSwapper.loadPlugin();
            BaseDataSourceDTO mysqlDto = new BaseDataSourceDTO();
            mysqlDto.setName("mysql5");
            mysqlDto.setHost("rmHost");
            mysqlDto.setPort("3304");
            mysqlDto.setDatabase("ag");
            mysqlDto.setUserName("root");
            mysqlDto.setPassword("951753");
            mysqlDto.setUsePool(true);

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("useSSL", "false");
            mysqlDto.setOther(JSONObject.toJSONString(jsonObject));
            Table<Map<String, Object>> agUser = mysql5.dataModelPreview(mysqlDto, "ag_user", "10");
            System.out.println(agUser);
        }
    }
}