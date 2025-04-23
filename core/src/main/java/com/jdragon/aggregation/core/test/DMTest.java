package com.jdragon.aggregation.core.test;

import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class DMTest {
    public static void main(String[] args) {
        BaseDataSourceDTO sourceDTO = new BaseDataSourceDTO();
        sourceDTO.setHost("172.20.10.2");
        sourceDTO.setPort("30236");
        sourceDTO.setDatabase("testdb");
        sourceDTO.setUserName("SYSDBA");
        sourceDTO.setPassword("SYSDBA");
        sourceDTO.setType("dm");
        try (PluginClassLoaderCloseable loaderSwapper =
                     PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, "dm")) {

            AbstractDataSourcePlugin sourcePlugin = loaderSwapper.loadPlugin();
            Connection connection = sourcePlugin.getConnection(sourceDTO);
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from TEST_1");
            while (resultSet.next()) {
                System.out.println(resultSet.getString(1));
                System.out.println(resultSet.getString(2));
            }
            connection.close();

            List<String> tables = sourcePlugin.getTableNames(sourceDTO, "");
            for (String table : tables) {
                System.out.println(table);
                String tableSize = sourcePlugin.getTableSize(sourceDTO, table);
                System.out.println("tableSize:" + tableSize);
                Long tableCount = sourcePlugin.getTableCount(sourceDTO, table);
                System.out.println("tableCount:" + tableCount);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
