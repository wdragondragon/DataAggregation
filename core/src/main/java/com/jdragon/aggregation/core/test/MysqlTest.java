package com.jdragon.aggregation.core.test;

import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.ColumnInfo;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;

import java.util.List;

public class MysqlTest {
    public static void main(String[] args) {
        BaseDataSourceDTO sourceDTO = new BaseDataSourceDTO();
        sourceDTO.setHost("172.20.10.2");
        sourceDTO.setPort("3306");
        sourceDTO.setDatabase("agg_test");
        sourceDTO.setUserName("root");
        sourceDTO.setPassword("951753");
        sourceDTO.setType("mysql8");
        try (PluginClassLoaderCloseable loaderSwapper =
                     PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, "mysql8")) {
            AbstractDataSourcePlugin sourcePlugin = loaderSwapper.loadPlugin();
            List<String> tables = sourcePlugin.getTableNames(sourceDTO, sourceDTO.getDatabase(), "");
            for (String table : tables) {
                System.out.println(table);
                List<ColumnInfo> columns = sourcePlugin.getColumns(sourceDTO, sourceDTO.getDatabase(), table);
                for (ColumnInfo column : columns) {
                    System.out.println(column);
                }
            }
        }
    }
}
