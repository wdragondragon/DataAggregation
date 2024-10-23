package com.jdragon.aggregation.datasource.rdbms;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.IDataSourceSql;
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
            if (log.isDebugEnabled()) {
                log.debug("start get {} connection, jdbcUrl={}", getType(), jdbcUrl);
            }
            if (dataSource.isUsePool()) {
                connection = DatasourceCache.getConnection(jdbcUrl, getDriver(), dataSource.getUserName(), dataSource.getPassword(), getTestQuery());
            } else {
                connection = JdbcSchema.dataSource(jdbcUrl, getDriver(), dataSource.getUserName(), dataSource.getPassword()).getConnection();
            }
            if (log.isDebugEnabled()) {
                log.debug("connection：{}", connection);
            }
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
        Table<Map<String, Object>> table = new Table<>();
        Map<String, String> fieldsMap = new HashMap<>();
        //获取连接
        Connection connection = getConnection(dataSource);
        PreparedStatement preparedStatement = null;
        ResultSet rs = null;
        try {
            String sql = MessageFormat.format(getDataSourceSql().getDataPreview(), tableName, limitSize);
            preparedStatement = connection.prepareStatement(sql);
            rs = preparedStatement.executeQuery();

            //获取列数
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            //设置body数据
            List<Map<String, Object>> resultData = resultSetToJsonObjList(rs);
            table.setBodies(resultData);

            //设置表头
            for (int i = 1; i <= columnCount; i++) {
                String fieldName = metaData.getColumnName(i);
                String fieldNameCn = StringUtils.isNotBlank(fieldsMap.get(fieldName)) ? fieldsMap.get(fieldName) : fieldName;
                table.addHeader(fieldName, fieldNameCn);
            }

        } catch (Exception e) {
            log.error("execute sql fail", e);
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            closeResource(rs, preparedStatement, connection);
        }
        return table;
    }

    public List<Map<String, Object>> resultSetToJsonObjList(ResultSet rs) throws Exception {
        // json数组
        List<Map<String, Object>> list = new ArrayList<>();

        // 获取列数
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        // 遍历ResultSet中的每条数据
        while (rs.next()) {
            Map<String, Object> jsonObj = new LinkedHashMap<>();
            // 遍历每一列
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnLabel(i);
                Object value = rs.getObject(columnName);
                if (value != null) {
                    value = resultSetToObject(value);
                }
                jsonObj.put(columnName, value);
            }
            list.add(jsonObj);
        }
        return list;
    }

    private final DecimalFormat DOUBLE_VALUE_FORMAT = new DecimalFormat("#.###############");

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    private static final SimpleDateFormat DATE_TIME_FORMAT2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public Object resultSetToObject(Object value) throws Exception {
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
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
