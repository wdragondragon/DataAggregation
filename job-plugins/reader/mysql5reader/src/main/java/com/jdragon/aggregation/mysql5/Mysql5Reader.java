package com.jdragon.aggregation.mysql5;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.mysql5.MysqlSourcePlugin;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class Mysql5Reader extends AbstractJobPlugin {
    @SneakyThrows
    @Override
    public void init() {
        MysqlSourcePlugin mysqlSourcePlugin = new MysqlSourcePlugin();
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

        Connection connection = mysqlSourcePlugin.getConnection(mysqlDto);

        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select * from ag_user");
        while (resultSet.next()) {
            System.out.println(resultSet.getString(2));
        }
        resultSet.close();
        statement.close();
        connection.close();
    }
}
