package com.jdragon.aggregation.datasource.rdbms;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.IDataSourceSql;
import com.jdragon.aggregation.datasource.InsertDataDTO;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Getter
@Slf4j
public abstract class RdbmsSourcePlugin extends AbstractDataSourcePlugin implements RdbmsSourceDefine {

    protected IDataSourceSql dataSourceSql;

    public RdbmsSourcePlugin(IDataSourceSql dataSourceSql) {
        this.dataSourceSql = dataSourceSql;
    }

    @Override
    public Connection getConnection(BaseDataSourceDTO dataSource) {
        Connection connection;
        try {
            String jdbcUrl = joinJdbcUrl(dataSource);
            log.info("start get {} connection, jdbcUrl={}", getType(), jdbcUrl);
            if (dataSource.isUsePool()) {
                connection = DatasourceCache.getConnection(jdbcUrl, getDriver(), dataSource.getUserName(), dataSource.getPassword(), getTestQuery());
            } else {
                connection = JdbcSchema.dataSource(jdbcUrl, getDriver(), dataSource.getUserName(), dataSource.getPassword()).getConnection();
            }
            log.info("connection success: {}", connection);
        } catch (Exception e) {
            log.error("get connection fail", e);
            throw new RuntimeException(e.getMessage(), e);
        }
        return connection;
    }

    @Override
    public String joinJdbcUrl(BaseDataSourceDTO dataSource) {
        String result = String.format(getJdbc(), dataSource.getHost(), dataSource.getPort(), dataSource.getDatabase());
        if (StringUtils.isNotBlank(dataSource.getOther())) {
            Map<String, String> map = JSONObject.parseObject(dataSource.getOther(), new TypeReference<LinkedHashMap<String, String>>() {
            });
            if (!map.isEmpty()) {
                Set<String> keys = map.keySet();
                StringBuilder str = new StringBuilder();
                for (String key : keys) {
                    str.append(String.format("%s=%s%s", key, map.get(key), getSeparator()));
                }
                str.deleteCharAt(str.length() - 1);
                result += getExtraParameterStart() + str;
            }

        }
        return result;
    }

    @Override
    public Table<Map<String, Object>> dataModelPreview(BaseDataSourceDTO dataSource, String tableName, String limitSize) {
        try {
            String sql = MessageFormat.format(getDataSourceSql().getDataPreview(), tableName, limitSize);
            //设置body数据
            return executeQuerySql(dataSource, sql, true);
        } catch (Exception e) {
            log.error("execute sql fail", e);
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public void insertData(InsertDataDTO insertDataDto) {
        Integer cacheSize = insertDataDto.getBatchSize();
        if (cacheSize == null) {
            cacheSize = 1024;
        }
        if (insertDataDto.isTruncate()) {
            String sql = MessageFormat.format(getDataSourceSql().getTruncateTable(), insertDataDto.getTableName());
            executeUpdate(insertDataDto.getBaseDataSourceDTO(), sql);
        }
        List<String> field = insertDataDto.getField();
        StringBuilder fields = new StringBuilder(getQuotationMarks() + field.get(0) + getQuotationMarks());
        for (int i = 1; i < field.size(); i++) {
            String s = getQuotationMarks() + field.get(i) + getQuotationMarks();
            fields.append(",").append(s);
        }

        List<String> valueCache = new LinkedList<>();
        for (List<String> dataN : insertDataDto.getData()) {
            for (int i = 0; i < dataN.size(); i++) {
                String columnValue = dataN.get(i);
                if (columnValue != null) {
                    columnValue = "'" + columnValue + "'";
                }
                dataN.set(i, columnValue);
            }
            valueCache.add("(" + StringUtils.join(dataN, ",") + ")");

            if (valueCache.size() >= cacheSize) {
                String sql = MessageFormat.format(getDataSourceSql().getInsertData(),
                        insertDataDto.getTableName(),
                        fields.toString(),
                        StringUtils.join(valueCache, ","));
                executeUpdate(insertDataDto.getBaseDataSourceDTO(), sql);
                valueCache.clear();
            }
        }
        if (!valueCache.isEmpty()) {
            String sql = MessageFormat.format(getDataSourceSql().getInsertData(),
                    insertDataDto.getTableName(),
                    fields.toString(),
                    StringUtils.join(valueCache, ","));
            executeUpdate(insertDataDto.getBaseDataSourceDTO(), sql);
            valueCache.clear();
        }
    }

    @Override
    public void executeUpdate(BaseDataSourceDTO dataSource, String sql) {
        try (Connection connection = getConnection(dataSource);
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.execute();
        } catch (SQLException e) {
            log.info(sql);
            log.error("execute query sql fail at MateDataService::excuteSql()", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public Table<Map<String, Object>> executeQuerySql(BaseDataSourceDTO dataSource, String sql, boolean columnLabel) {
        Table<Map<String, Object>> table = new Table<>();
        ResultSet resultSet = null;
        List<Map<String, Object>> resultList = new ArrayList<>();
        Connection connection = null;
        Statement statement = null;
        try {
            connection = getConnection(dataSource);
            statement = connection.createStatement();
            statement.setEscapeProcessing(false);
            log.info("execute query: {}", sql);
            resultSet = statement.executeQuery(sql);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> mapOfColValues = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    Object value = resultSet.getObject(columnName);
                    value = resultSetToObject(value);
                    String fieldName = columnLabel ? metaData.getColumnLabel(i) : metaData.getColumnName(i);
                    mapOfColValues.put(fieldName, value);
                }
                resultList.add(mapOfColValues);
            }
            //设置表头
            for (int i = 1; i <= columnCount; i++) {
                String fieldName = columnLabel ? metaData.getColumnLabel(i) : metaData.getColumnName(i);
                table.addHeader(fieldName, fieldName);
            }
        } catch (Exception e) {
            log.info(sql);
            log.error("execute query sql fail at MateDataService::excuteSql()", e);
            throw new RuntimeException(e.getMessage());
        } finally {
            closeResource(resultSet, statement, connection);
        }
        table.setBodies(resultList);
        return table;
    }

    private final DecimalFormat DOUBLE_VALUE_FORMAT = new DecimalFormat("#.###############");

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private static final SimpleDateFormat DATE_TIME_FORMAT2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Object resultSetToObject(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Double || value instanceof Float) {
            String val = value.toString();
            if (val.toUpperCase().contains("E")) {
                value = DOUBLE_VALUE_FORMAT.format(value);
            }
        } else if (value instanceof java.sql.Timestamp) {
            return DATE_TIME_FORMAT2.format(value);
        } else if (value instanceof java.sql.Date) {
            return DATE_TIME_FORMAT.format(value);
        } else if (value instanceof java.time.LocalDateTime) {
            return ((java.time.LocalDateTime) value).format(DATE_TIME_FORMATTER);
        } else if (value instanceof byte[]) {
            value = "(BLOB)  " + ((byte[]) value).length + " bytes";
        }
        return value;
    }

    protected void closeResource(ResultSet rs, Statement ps, Connection conn) {
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
        if (conn != null) {
            try {
                conn.close();
                log.info("close connection success");
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
