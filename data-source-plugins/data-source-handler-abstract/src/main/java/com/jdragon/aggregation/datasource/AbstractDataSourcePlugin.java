package com.jdragon.aggregation.datasource;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public abstract class AbstractDataSourcePlugin extends AbstractPlugin {

    public Connection getConnection(Configuration configuration) {
        String json = configuration.toJSON();
        return getConnection(JSONObject.parseObject(json, BaseDataSourceDTO.class));
    }

    public abstract Connection getConnection(BaseDataSourceDTO dataSource);

    public abstract Table<Map<String, Object>> executeQuerySql(BaseDataSourceDTO dataSource, String sql, boolean columnLabel);

    public void scanQuery(BaseDataSourceDTO dataSource, String sql, boolean columnLabel, Consumer<Map<String, Object>> rowConsumer) {
        Table<Map<String, Object>> table = executeQuerySql(dataSource, sql, columnLabel);
        if (table == null || table.getBodies() == null || rowConsumer == null) {
            return;
        }
        for (Map<String, Object> row : table.getBodies()) {
            rowConsumer.accept(row);
        }
    }

    public abstract void executeUpdate(BaseDataSourceDTO dataSource, String sql);

    public abstract void executeBatch(BaseDataSourceDTO dataSource, List<String> sqlList);

    public abstract Table<Map<String, Object>> dataModelPreview(BaseDataSourceDTO dataSource, String tableName, String limitSize);

    public abstract void insertData(InsertDataDTO insertDataDTO);

    public abstract List<TableInfo> getTableInfos(BaseDataSourceDTO dataSource, String table);

    public Map<String, List<TableInfo>> getTableInfos(BaseDataSourceDTO dataSource, List<String> tables) {
        Map<String, List<TableInfo>> result = new LinkedHashMap<String, List<TableInfo>>();
        if (tables == null) {
            return result;
        }
        for (String table : tables) {
            if (table == null || table.trim().isEmpty()) {
                continue;
            }
            result.put(table, getTableInfos(dataSource, table));
        }
        return result;
    }

    public abstract List<String> getTableNames(BaseDataSourceDTO dataSource, String table);

    public abstract List<ColumnInfo> getColumns(BaseDataSourceDTO dataSource, String table);

    public Map<String, List<ColumnInfo>> getColumns(BaseDataSourceDTO dataSource, List<String> tables) {
        Map<String, List<ColumnInfo>> result = new LinkedHashMap<String, List<ColumnInfo>>();
        if (tables == null) {
            return result;
        }
        for (String table : tables) {
            if (table == null || table.trim().isEmpty()) {
                continue;
            }
            result.put(table, getColumns(dataSource, table));
        }
        return result;
    }

    /**
     * 获取表大小，单位为mb
     */
    public abstract String getTableSize(BaseDataSourceDTO dataSource, String table);

    public abstract Long getTableCount(BaseDataSourceDTO dataSource, String table);

    public boolean connectTest(BaseDataSourceDTO dataSource) {
        throw new UnsupportedOperationException("该数据源不支持连接测试");
    }

    public List<PartitionInfo> getPartitionInfo(BaseDataSourceDTO dataSource, String table) {
        return new ArrayList<>();
    }
}
