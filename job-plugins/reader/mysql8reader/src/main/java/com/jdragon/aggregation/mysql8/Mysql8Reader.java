package com.jdragon.aggregation.mysql8;

import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.mysql8.Mysql8SourcePlugin;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class Mysql8Reader extends AbstractJobPlugin {
    @Override
    public void init() {
        try {
            Mysql8SourcePlugin mysqlSourcePlugin = new Mysql8SourcePlugin();
            BaseDataSourceDTO mysqlDto = new BaseDataSourceDTO();
            mysqlDto.setName("mysql8");
            mysqlDto.setHost("rmHost");
            mysqlDto.setPort("3305");
            mysqlDto.setDatabase("ag");
            mysqlDto.setUserName("root");
            mysqlDto.setPassword("951753");
            mysqlDto.setUsePool(true);
            Connection connection = mysqlSourcePlugin.getConnection(mysqlDto);

            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select * from ag_user");
            while (resultSet.next()) {
                System.out.println(resultSet.getString(2));
            }
            resultSet.close();
            statement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
