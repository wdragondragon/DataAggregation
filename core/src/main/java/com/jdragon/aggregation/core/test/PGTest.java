package com.jdragon.aggregation.core.test;

import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;

import java.util.List;

public class PGTest {
    public static void main(String[] args) {
        BaseDataSourceDTO sourceDTO = new BaseDataSourceDTO();
        sourceDTO.setHost("172.20.10.2");
        sourceDTO.setPort("15432");
        sourceDTO.setDatabase("jdragon");
        sourceDTO.setUserName("jdragon");
        sourceDTO.setPassword("Zhjl.postgres");
        sourceDTO.setType("postgres");
        try (PluginClassLoaderCloseable loaderSwapper =
                     PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, "postgres")) {
            AbstractDataSourcePlugin sourcePlugin = loaderSwapper.loadPlugin();
            List<String> tables = sourcePlugin.getTableNames(sourceDTO, "");
            for (String table : tables) {
                System.out.println(table);
                String tableSize = sourcePlugin.getTableSize(sourceDTO, table);
                System.out.println("tableSize:" + tableSize);
                Long tableCount = sourcePlugin.getTableCount(sourceDTO, table);
                System.out.println("tableCount:" + tableCount);
            }
        }
    }
}
