package com.jdragon.aggregation.rdbms.writer;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.TimestampColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.DataSourceType;
import com.jdragon.aggregation.datasource.rdbms.RdbmsSourcePlugin;
import com.jdragon.aggregation.rdbms.util.DBUtil;
import com.jdragon.aggregation.rdbms.util.DBUtilErrorCode;
import com.jdragon.aggregation.rdbms.util.WriterUtil;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

@Getter
public class CommonRdbmsWriter extends Writer.Job {
    protected static final Logger LOG = LoggerFactory
            .getLogger(CommonRdbmsWriter.class);

    private BaseDataSourceDTO dataSource;

    private final RdbmsSourcePlugin sourcePlugin;

    private String insertSql;

    private int batchSize;

    private int columnNumber;

    private String tableName;

    private List<String> columns;

    protected boolean emptyAsNull;

    private DataSourceType dataSourceType;

    private String writeMode;

    private List<String> pks;

    private static final String VALUE_HOLDER = "?";

    public CommonRdbmsWriter(RdbmsSourcePlugin sourcePlugin) {
        this.sourcePlugin = sourcePlugin;
    }

    @Override
    public void init() {
        dataSourceType = sourcePlugin.getType();
        Configuration connectConfig = this.getPluginJobConf().getConfiguration("connect");
        dataSource = JSONObject.parseObject(connectConfig.toString(), BaseDataSourceDTO.class);
        dataSource.setName(DataSourceType.Mysql8.getTypeName());
        tableName = this.getPluginJobConf().getString("table");
        columns = this.getPluginJobConf().getList("columns", String.class);
        writeMode = this.getPluginJobConf().getString("writeMode", "INSERT");
        pks = getPluginJobConf().getList("pkColumn", new ArrayList<>(), String.class);
        batchSize = this.getPluginJobConf().getInt("batchSize", 1024);
        emptyAsNull = this.getPluginJobConf().getBool("emptyAsNull", false);

        columnNumber = columns.size();
        if (columnNumber == 1 && columns.get(0).equalsIgnoreCase("*")) {
            try (Connection connection = DBUtil.getConnection(sourcePlugin, dataSource)) {
                this.resultSetMetaData = DBUtil.getColumnMetaData(connection,
                        this.tableName, "*");
                columns = this.resultSetMetaData.getLeft();
                columnNumber = columns.size();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        calcWriteRecordSql();
        LOG.info("rdbms writer template: {}", insertSql);
    }

    protected void calcWriteRecordSql() {
        List<String> valueHolders = new ArrayList<String>(columnNumber);
        for (int i = 0; i < columns.size(); i++) {
            valueHolders.add(VALUE_HOLDER);
        }

        boolean forceUseUpdate = dataSourceType != null
                && (dataSourceType == DataSourceType.MySql || dataSourceType == DataSourceType.Mysql8);

        String insertSqlTemplate = WriterUtil.getWriteTemplate(columns, valueHolders, writeMode, sourcePlugin, forceUseUpdate, pks);
        insertSql = String.format(insertSqlTemplate, this.tableName);
    }

    protected String calcValueHolder(String columnType) {
        return VALUE_HOLDER;
    }

    @Override
    public void prepare() {
        if (this.resultSetMetaData == null) {
            try (Connection connection = DBUtil.getConnection(sourcePlugin, dataSource)) {
                this.resultSetMetaData = DBUtil.getColumnMetaData(connection,
                        this.tableName, StringUtils.join(DBUtil.handleKeywords(sourcePlugin, this.columns), ","));
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }


    protected Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

    @Override
    public void startWrite(RecordReceiver recordReceiver) {
        Connection connection = DBUtil.getConnection(sourcePlugin, dataSource);
        List<Record> writeBuffer = new ArrayList<>(batchSize);
        try {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                writeBuffer.add(record);
                if (writeBuffer.size() >= batchSize) {
                    connection = doInsertWithRetry(connection, writeBuffer);
                    writeBuffer.clear();
                    this.getJobPointReporter().report();
                }
            }
            if (!writeBuffer.isEmpty()) {
                connection = doInsertWithRetry(connection, writeBuffer);
                writeBuffer.clear();
                this.getJobPointReporter().report();
            }
        } catch (Exception e) {
            throw AggregationException.asException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            writeBuffer.clear();
            DBUtil.closeDBResources(null, null, connection);
        }
    }

    // 直接使用了两个类变量：columnNumber,resultSetMetaData
    protected PreparedStatement fillPreparedStatement(PreparedStatement preparedStatement, Record record)
            throws SQLException {
        for (int i = 0; i < this.columnNumber; i++) {
            int columnSqltype = this.resultSetMetaData.getMiddle().get(i);
            Column column = record.getColumn(i);
            if (column == null) {
                preparedStatement.setObject(i + 1, null);
                continue;
            }
            preparedStatement = fillPreparedStatementColumnType(preparedStatement, i, columnSqltype, column);
        }

        return preparedStatement;
    }

    protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex, int columnSqltype, Column column) throws SQLException {
        switch (columnSqltype) {
            case Types.CHAR:
            case Types.NCHAR:
            case Types.NCLOB:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.SQLXML:
                if (null == column.getRawData()) {
                    preparedStatement.setObject(columnIndex + 1, null);
                } else {
                    preparedStatement.setString(columnIndex + 1,
                            column.asString());
                }
                break;
            case Types.CLOB:
                preparedStatement.setBinaryStream(columnIndex + 1, new ByteArrayInputStream(column.asBytes()));
                break;
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.TINYINT:
                String strValue = column.asString();
                if (emptyAsNull && "".equals(strValue)) {
                    preparedStatement.setString(columnIndex + 1, null);
                } else if (null == column.getRawData()) {
                    preparedStatement.setObject(columnIndex + 1, null);
                } else {
                    preparedStatement.setLong(columnIndex + 1, column.asLong());
                }
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                if (emptyAsNull && "".equals(column.asString())) {
                    preparedStatement.setString(columnIndex + 1, null);
                } else if (null == column.getRawData()) {
                    preparedStatement.setObject(columnIndex + 1, null);
                } else {
                    BigDecimal bigDecimal = column.asBigDecimal();
                    preparedStatement.setString(columnIndex + 1, bigDecimal.toString());
                }
                break;
            // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
            case Types.DATE:
                if (this.resultSetMetaData.getRight().get(columnIndex)
                        .equalsIgnoreCase("year")) {
                    if (column.asBigInteger() == null) {
                        preparedStatement.setString(columnIndex + 1, null);
                    } else {
                        preparedStatement.setInt(columnIndex + 1, column.asBigInteger().intValue());
                    }
                } else if (column.getByteSize() == 0) {
                    preparedStatement.setDate(columnIndex + 1, null);
                } else {
                    preparedStatement.setDate(columnIndex + 1, parseSqlDate(column));
                }
                break;

            case Types.TIME:
                if (column.getByteSize() == 0) {
                    preparedStatement.setTime(columnIndex + 1, null);
                    break;
                }
                preparedStatement.setTime(columnIndex + 1, parseSqlTime(column));
                break;
            case Types.TIMESTAMP:
                if (column.getByteSize() == 0) {
                    preparedStatement.setTimestamp(columnIndex + 1, null);
                    break;
                }
                if (column instanceof TimestampColumn) {
                    preparedStatement.setTimestamp(columnIndex + 1, ((TimestampColumn) column).asTimestamp());
                    break;
                }
                preparedStatement.setTimestamp(columnIndex + 1, parseSqlTimestamp(column));
                break;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                if (null == column.getRawData()) {
                    preparedStatement.setObject(columnIndex + 1, null);
                } else {
                    preparedStatement.setBytes(columnIndex + 1,
                            column.asBytes());
                }
                break;
            case Types.OTHER:
                preparedStatement.setObject(columnIndex + 1, column.asString());
                break;
            case Types.BOOLEAN:
                if (null == column.getRawData()) {
                    preparedStatement.setNull(columnIndex + 1,
                            Types.BOOLEAN);
                } else {
                    preparedStatement.setBoolean(columnIndex + 1,
                            column.asBoolean());
                }
                break;

            // warn: bit(1) -> Types.BIT 可使用setBoolean
            // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
            case Types.BIT:
                if (null == column.getRawData()) {
                    preparedStatement.setObject(columnIndex + 1, null);
                } else if (this.dataSourceType == DataSourceType.MySql || this.dataSourceType == DataSourceType.Mysql8) {
                    preparedStatement.setBoolean(columnIndex + 1,
                            column.asBoolean());
                } else if (this.dataSourceType == DataSourceType.PostgreSQL) {
                    preparedStatement.setString(columnIndex + 1, column.asLong().toString());
                } else {
                    preparedStatement.setString(columnIndex + 1,
                            column.asString());
                }
                break;
            default:
                throw AggregationException
                        .asException(
                                DBUtilErrorCode.UNSUPPORTED_TYPE,
                                String.format(
                                        "您的配置文件中的列配置信息有误. 因为引擎 不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%d], 字段Java类型:[%s]. 请修改表中该字段的类型或者不同步该字段.",
                                        this.resultSetMetaData.getLeft()
                                                .get(columnIndex),
                                        this.resultSetMetaData.getMiddle()
                                                .get(columnIndex),
                                        this.resultSetMetaData.getRight()
                                                .get(columnIndex)));
        }
        return preparedStatement;
    }

    protected java.sql.Date parseSqlDate(Column column) throws SQLException {
        if (isNullOrBlank(column)) {
            return null;
        }
        Object raw = column.getRawData();
        if (raw instanceof java.sql.Date) {
            return (java.sql.Date) raw;
        }
        if (raw instanceof java.util.Date) {
            return new java.sql.Date(((java.util.Date) raw).getTime());
        }
        String text = column.asString();
        try {
            if (isLongNumber(text)) {
                return new java.sql.Date(Long.parseLong(text.trim()));
            }
            return java.sql.Date.valueOf(extractDatePart(text));
        } catch (Exception ignored) {
            try {
                java.util.Date utilDate = column.asDate();
                return utilDate == null ? null : new java.sql.Date(utilDate.getTime());
            } catch (Exception e) {
                throw new SQLException(String.format("Date 类型转换错误：[%s]", column), e);
            }
        }
    }

    protected java.sql.Time parseSqlTime(Column column) throws SQLException {
        if (isNullOrBlank(column)) {
            return null;
        }
        Object raw = column.getRawData();
        if (raw instanceof java.sql.Time) {
            return (java.sql.Time) raw;
        }
        if (raw instanceof java.util.Date) {
            return new java.sql.Time(((java.util.Date) raw).getTime());
        }
        String text = column.asString();
        try {
            if (isLongNumber(text)) {
                return new java.sql.Time(Long.parseLong(text.trim()));
            }
            return java.sql.Time.valueOf(normalizeTimeText(text));
        } catch (Exception ignored) {
            try {
                java.util.Date utilDate = column.asDate();
                return utilDate == null ? null : new java.sql.Time(utilDate.getTime());
            } catch (Exception e) {
                throw new SQLException(String.format("TIME 类型转换错误：[%s]", column), e);
            }
        }
    }

    protected java.sql.Timestamp parseSqlTimestamp(Column column) throws SQLException {
        if (isNullOrBlank(column)) {
            return null;
        }
        Object raw = column.getRawData();
        if (raw instanceof java.sql.Timestamp) {
            return (java.sql.Timestamp) raw;
        }
        if (raw instanceof java.util.Date) {
            return new java.sql.Timestamp(((java.util.Date) raw).getTime());
        }
        String text = column.asString();
        try {
            if (isLongNumber(text)) {
                return new java.sql.Timestamp(Long.parseLong(text.trim()));
            }
            return java.sql.Timestamp.valueOf(normalizeTimestampText(text));
        } catch (Exception ignored) {
            try {
                java.util.Date utilDate = column.asDate();
                return utilDate == null ? null : new java.sql.Timestamp(utilDate.getTime());
            } catch (Exception e) {
                throw new SQLException(String.format("TIMESTAMP 类型转换错误：[%s]", column), e);
            }
        }
    }

    private boolean isNullOrBlank(Column column) {
        return column == null || column.getRawData() == null || column.getByteSize() == 0
                || (column.asString() != null && column.asString().trim().isEmpty());
    }

    private boolean isLongNumber(String text) {
        if (text == null) {
            return false;
        }
        String trimmed = text.trim();
        if (trimmed.isEmpty()) {
            return false;
        }
        int start = trimmed.charAt(0) == '-' ? 1 : 0;
        if (start == trimmed.length()) {
            return false;
        }
        for (int i = start; i < trimmed.length(); i++) {
            if (!Character.isDigit(trimmed.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private String extractDatePart(String text) {
        String normalized = text.trim().replace('T', ' ');
        int space = normalized.indexOf(' ');
        return space >= 0 ? normalized.substring(0, space) : normalized;
    }

    private String normalizeTimeText(String text) {
        String normalized = text.trim().replace('T', ' ');
        int space = normalized.indexOf(' ');
        if (space >= 0) {
            normalized = normalized.substring(space + 1).trim();
        }
        LocalTime time = LocalTime.parse(appendSecondsIfMissing(normalized));
        return time.toString().length() == 5 ? time + ":00" : time.toString();
    }

    private String normalizeTimestampText(String text) {
        String normalized = text.trim().replace('T', ' ');
        if (normalized.length() == 10) {
            return LocalDate.parse(normalized).toString() + " 00:00:00";
        }
        int space = normalized.indexOf(' ');
        if (space < 0) {
            return normalized;
        }
        String date = normalized.substring(0, space);
        String time = appendSecondsIfMissing(normalized.substring(space + 1).trim());
        LocalDateTime.parse(date + "T" + time);
        return date + " " + time;
    }

    private String appendSecondsIfMissing(String time) {
        if (time == null) {
            return "";
        }
        String normalized = time.trim();
        int dot = normalized.indexOf('.');
        String main = dot >= 0 ? normalized.substring(0, dot) : normalized;
        String fraction = dot >= 0 ? normalized.substring(dot) : "";
        int colonCount = 0;
        for (int i = 0; i < main.length(); i++) {
            if (main.charAt(i) == ':') {
                colonCount++;
            }
        }
        return colonCount == 1 ? main + ":00" + fraction : normalized;
    }

    private Connection doInsertWithRetry(Connection connection, List<Record> writeBuffer) throws SQLException {
        int retry = 0;
        while (true) {
            try {
                doBatchInsert(connection, writeBuffer);
                break;
            } catch (SQLException e) {
                if (++retry > 3 || !e.getMessage().contains("connection")) {
                    throw e;
                } else {
                    connection = reopenConnection(connection);
                }
            }
        }
        return connection;
    }

    protected void doBatchInsert(Connection connection, List<Record> buffer)
            throws SQLException {
        PreparedStatement preparedStatement = null;
        try {
            connection.setAutoCommit(false);
            preparedStatement = connection
                    .prepareStatement(this.insertSql);

            for (Record record : buffer) {
                preparedStatement = fillPreparedStatement(
                        preparedStatement, record);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            LOG.warn("回滚此次写入, 采用每次写入一行方式提交. 因为:" + e.getMessage());
            connection.rollback();
            doOneInsert(connection, buffer);
        } catch (Exception e) {
            throw AggregationException.asException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            DBUtil.closeDBResources(preparedStatement, null);
        }
    }

    protected void doOneInsert(Connection connection, List<Record> buffer) throws SQLException {
        PreparedStatement preparedStatement = null;
        SQLException firstFailure = null;
        int failureCount = 0;
        try {
            connection.setAutoCommit(true);
            preparedStatement = connection
                    .prepareStatement(this.insertSql);

            int rowIndex = 0;
            for (Record record : buffer) {
                rowIndex++;
                try {
                    preparedStatement = fillPreparedStatement(
                            preparedStatement, record);
                    preparedStatement.execute();
                } catch (SQLException e) {
                    failureCount++;
                    if (firstFailure == null) {
                        firstFailure = e;
                    }
                    LOG.warn("逐行写入失败, rowIndex={}, error={}", rowIndex, e.getMessage(), e);
                    // 脏数据收集
                } finally {
                    // 最后不要忘了关闭 preparedStatement
                    preparedStatement.clearParameters();
                }
            }
            if (firstFailure != null) {
                throw new SQLException(String.format("逐行写入失败，共 %d 条记录写入失败", failureCount), firstFailure);
            }
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw AggregationException.asException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        } finally {
            DBUtil.closeDBResources(preparedStatement, null);
        }
    }

    private Connection reopenConnection(Connection connection) {
        DBUtil.closeDBResources(null, null, connection);
        LOG.warn("连接异常，重新建立数据库连接，将会重试10次");
        //重新获取连接
        return DBUtil.getConnection(sourcePlugin, dataSource);
    }

    @Override
    public void post() {
        super.post();
    }

    @Override
    public void destroy() {
        super.destroy();
    }
}
