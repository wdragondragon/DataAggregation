package com.jdragon.aggregation.datasource.http;

import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.ColumnInfo;
import com.jdragon.aggregation.datasource.InsertDataDTO;
import com.jdragon.aggregation.datasource.TableInfo;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.unstructuredstorage.ColumnEntry;

import java.sql.Connection;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class HttpSourcePlugin extends AbstractDataSourcePlugin {
    private final HttpSourceScanner scanner = new HttpSourceScanner();

    @Override
    public Connection getConnection(BaseDataSourceDTO dataSource) {
        return null;
    }

    @Override
    public Table<Map<String, Object>> executeQuerySql(BaseDataSourceDTO dataSource, String sql, boolean columnLabel) {
        Table<Map<String, Object>> table = new Table<>();
        List<Map<String, Object>> rows = new ArrayList<>();
        scanQuery(dataSource, sql, columnLabel, rows::add);
        table.setBodies(rows);
        if (!rows.isEmpty()) {
            for (String key : rows.get(0).keySet()) {
                table.addHeader(key, key);
            }
        }
        return table;
    }

    @Override
    public void scanQuery(BaseDataSourceDTO dataSource, String sql, boolean columnLabel, Consumer<Map<String, Object>> rowConsumer) {
        scanRows(dataSource, rowConsumer, resolveMaxRows(dataSource));
    }

    private void scanRows(BaseDataSourceDTO dataSource, Consumer<Map<String, Object>> rowConsumer, int maxRows) {
        HttpSourceConfig config = resolveConfig(dataSource);
        List<HttpPushdownPredicate> predicates = resolvePredicates(dataSource);
        scanner.scan(scanner.applyPushdown(config, predicates), rowConsumer, maxRows);
    }

    @Override
    public void executeUpdate(BaseDataSourceDTO dataSource, String sql) {
        throw new UnsupportedOperationException("HTTP source does not support update");
    }

    @Override
    public void executeBatch(BaseDataSourceDTO dataSource, List<String> sqlList) {
        throw new UnsupportedOperationException("HTTP source does not support batch update");
    }

    @Override
    public Table<Map<String, Object>> dataModelPreview(BaseDataSourceDTO dataSource, String tableName, String limitSize) {
        Table<Map<String, Object>> table = new Table<>();
        List<Map<String, Object>> rows = new ArrayList<>();
        scanRows(dataSource, rows::add, positiveLimit(limitSize));
        table.setBodies(rows);
        if (!rows.isEmpty()) {
            for (String key : rows.get(0).keySet()) {
                table.addHeader(key, key);
            }
        }
        return table;
    }

    @Override
    public void insertData(InsertDataDTO insertDataDTO) {
        throw new UnsupportedOperationException("HTTP source does not support insert");
    }

    @Override
    public List<TableInfo> getTableInfos(BaseDataSourceDTO dataSource, String table) {
        TableInfo info = new TableInfo();
        info.setTableName(table == null || table.trim().isEmpty() ? "http" : table);
        info.setTableType("HTTP");
        return Collections.singletonList(info);
    }

    @Override
    public List<String> getTableNames(BaseDataSourceDTO dataSource, String table) {
        return Collections.singletonList(table == null || table.trim().isEmpty() ? "http" : table);
    }

    @Override
    public List<ColumnInfo> getColumns(BaseDataSourceDTO dataSource, String table) {
        HttpSourceConfig config = resolveConfig(dataSource);
        List<ColumnInfo> columns = new ArrayList<>();
        if (config.getColumns() == null) {
            return columns;
        }
        for (ColumnEntry entry : config.getColumns()) {
            ColumnInfo info = new ColumnInfo();
            info.setColumnName(entry.getName());
            info.setTypeName(entry.getType());
            info.setDataType(Types.VARCHAR);
            columns.add(info);
        }
        return columns;
    }

    @Override
    public String getTableSize(BaseDataSourceDTO dataSource, String table) {
        return "0";
    }

    @Override
    public Long getTableCount(BaseDataSourceDTO dataSource, String table) {
        return 0L;
    }

    private HttpSourceConfig resolveConfig(BaseDataSourceDTO dataSource) {
        Map<String, String> extraParams = dataSource == null ? null : dataSource.getExtraParams();
        if (extraParams != null && extraParams.get(HttpSourceKeys.STUDIO_READER_CONFIG) != null) {
            return HttpSourceConfig.fromJson(extraParams.get(HttpSourceKeys.STUDIO_READER_CONFIG));
        }
        Map<String, Object> config = new LinkedHashMap<>();
        if (extraParams != null) {
            config.putAll(extraParams);
        }
        return HttpSourceConfig.fromConfiguration(Configuration.from(config));
    }

    private List<HttpPushdownPredicate> resolvePredicates(BaseDataSourceDTO dataSource) {
        Map<String, String> extraParams = dataSource == null ? null : dataSource.getExtraParams();
        if (extraParams == null) {
            return Collections.emptyList();
        }
        return HttpPushdownPredicate.parseList(extraParams.get(HttpSourceKeys.STUDIO_PUSHDOWN_FILTERS));
    }

    private int positiveLimit(String limitSize) {
        if (limitSize == null || limitSize.trim().isEmpty()) {
            return 0;
        }
        try {
            int value = Integer.parseInt(limitSize.trim());
            return Math.max(value, 0);
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }

    private int resolveMaxRows(BaseDataSourceDTO dataSource) {
        Map<String, String> extraParams = dataSource == null ? null : dataSource.getExtraParams();
        return positiveLimit(extraParams == null ? null : extraParams.get(HttpSourceKeys.STUDIO_MAX_ROWS));
    }
}
