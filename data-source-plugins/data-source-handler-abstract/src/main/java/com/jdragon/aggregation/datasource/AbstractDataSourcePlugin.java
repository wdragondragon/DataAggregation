package com.jdragon.aggregation.datasource;

import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;

import java.sql.Connection;
import java.util.Map;

public abstract class AbstractDataSourcePlugin extends AbstractPlugin {

    public abstract Connection getConnection(BaseDataSourceDTO dataSource);

    public abstract Table<Map<String, Object>> executeQuerySql(BaseDataSourceDTO dataSource, String sql, boolean columnLabel);

    public abstract void executeUpdate(BaseDataSourceDTO dataSource, String sql);

    public abstract Table<Map<String, Object>> dataModelPreview(BaseDataSourceDTO dataSource, String tableName, String limitSize);

    public abstract void insertData(InsertDataDTO insertDataDTO);

}
