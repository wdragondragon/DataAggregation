package com.jdragon.aggregation.core;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;

import java.io.File;
import java.util.Map;

public class Main {

    private static final String configPath = System.getProperty("configPath", "source.json");

    private static final String execSql = System.getProperty("execSql");

    private static final Configuration configuration = Configuration.from(new File(configPath));

    public static void main(String[] args) {
        String json = configuration.toJSON();
        BaseDataSourceDTO dto = JSONObject.parseObject(json, BaseDataSourceDTO.class);
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, dto.getName())) {
            AbstractDataSourcePlugin mysql5 = classLoaderSwapper.loadPlugin();
            Table<Map<String, Object>> mapTable = mysql5.executeQuerySql(dto, execSql, true);
            System.out.println(mapTable);
        }
    }
}