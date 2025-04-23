package com.jdragon.aggregation.datasource;

import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public abstract class AbstractDataSourcePlugin extends AbstractPlugin {

    public abstract Connection getConnection(BaseDataSourceDTO dataSource);

    public abstract Table<Map<String, Object>> executeQuerySql(BaseDataSourceDTO dataSource, String sql, boolean columnLabel);

    public abstract void executeUpdate(BaseDataSourceDTO dataSource, String sql);

    public abstract void executeBatch(BaseDataSourceDTO dataSource, List<String> sqlList);

    public abstract Table<Map<String, Object>> dataModelPreview(BaseDataSourceDTO dataSource, String tableName, String limitSize);

    public abstract void insertData(InsertDataDTO insertDataDTO);

    public abstract List<TableInfo> getTableInfos(BaseDataSourceDTO dataSource, String table);

    public abstract List<String> getTableNames(BaseDataSourceDTO dataSource, String table);

    public abstract List<ColumnInfo> getColumns(BaseDataSourceDTO dataSource, String table);

    /**
     * 获取表大小，单位为mb
     */
    public abstract String getTableSize(BaseDataSourceDTO dataSource, String table);

    public abstract Long getTableCount(BaseDataSourceDTO dataSource, String table);
}
