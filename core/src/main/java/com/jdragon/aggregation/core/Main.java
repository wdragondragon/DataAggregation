package com.jdragon.aggregation.core;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.InsertDataDTO;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Main {

    private static final String sourceConfig = System.getProperty("sourceConfig", "source.json");

    private static final String targetConfig = System.getProperty("targetConfig", "source.json");

    private static final String execSql = System.getProperty("execSql");

    private static final Configuration sourceConfiguration = Configuration.from(new File(sourceConfig));

    private static final Configuration targetConfiguration = Configuration.from(new File(targetConfig));

    public static void main(String[] args) {
        Table<Map<String, Object>> mapTable;
        String json = sourceConfiguration.toJSON();
        BaseDataSourceDTO sourceDTO = JSONObject.parseObject(json, BaseDataSourceDTO.class);
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, sourceDTO.getName())) {
            AbstractDataSourcePlugin plugin = classLoaderSwapper.loadPlugin();
            mapTable = plugin.executeQuerySql(sourceDTO, execSql, true);
        }
        if (mapTable == null) {
            return;
        }

        List<String> fieldNames = mapTable.getHeaders().stream().map(Table.Header::getName).collect(Collectors.toList());
        List<Map<String, Object>> bodies = mapTable.getBodies();
        List<List<String>> dataList = bodies.stream()
                .map(body -> fieldNames.stream()
                        .map(fieldName -> {
                            Object o = body.get(fieldName);
                            return o == null ? null : o.toString();
                        })
                        .collect(Collectors.toList()))
                .collect(Collectors.toList());


        BaseDataSourceDTO targetDTO = JSONObject.parseObject(targetConfiguration.toJSON(), BaseDataSourceDTO.class);
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, targetDTO.getName())) {
            AbstractDataSourcePlugin plugin = classLoaderSwapper.loadPlugin();
            InsertDataDTO insertDataDTO = new InsertDataDTO(fieldNames, dataList);
            insertDataDTO.setTableName("test");
            insertDataDTO.setBaseDataSourceDTO(targetDTO);
            insertDataDTO.setTruncate(true);
            plugin.insertData(insertDataDTO);
        }
    }
}