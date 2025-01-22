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
import java.util.ArrayList;
import java.util.List;

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
        columnNumber = columns.size();
        List<String> valueHolders = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            valueHolders.add("?");
        }
        String insertSqlTemplate = "INSERT INTO %s (" + StringUtils.join(columns, ",") +
                ") VALUES(" + StringUtils.join(valueHolders, ",") +
                ")" + onDuplicateKeyUpdateString(columns);

        insertSql = String.format(insertSqlTemplate, tableName);
        batchSize = this.getPluginJobConf().getInt("batchSize", 1024);
        emptyAsNull = this.getPluginJobConf().getBool("emptyAsNull", false);

    }

    public static String onDuplicateKeyUpdateString(List<String> columnHolders) {
        if (columnHolders == null || columnHolders.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append(" ON DUPLICATE KEY UPDATE ");
        boolean first = true;
        for (String column : columnHolders) {
            if (!first) {
                sb.append(",");
            } else {
                first = false;
            }
            sb.append(column);
            sb.append("=VALUES(");
            sb.append(column);
            sb.append(")");
        }

        return sb.toString();
    }

    @Override
    public void prepare() {
        Connection connection = sourcePlugin.getConnection(dataSource);
        this.resultSetMetaData = DBUtil.getColumnMetaData(connection,
                this.tableName, StringUtils.join(DBUtil.handleKeywords(sourcePlugin, this.columns), ","));
    }


    protected Triple<List<String>, List<Integer>, List<String>> resultSetMetaData;

    @Override
    public void startWrite(RecordReceiver recordReceiver) {
        Connection connection = sourcePlugin.getConnection(dataSource);
        List<Record> writeBuffer = new ArrayList<>(batchSize);
        try {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                writeBuffer.add(record);
                if (writeBuffer.size() >= batchSize) {
                    connection = doInsertWithRetry(connection, writeBuffer);
                    writeBuffer.clear();
                }
            }
            if (!writeBuffer.isEmpty()) {
                connection = doInsertWithRetry(connection, writeBuffer);
                writeBuffer.clear();
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
            preparedStatement = fillPreparedStatementColumnType(preparedStatement, i, columnSqltype, record.getColumn(i));
        }

        return preparedStatement;
    }

    protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex, int columnSqltype, Column column) throws SQLException {
        java.util.Date utilDate;
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
                    java.sql.Date sqlDate = null;
                    try {
                        utilDate = column.asDate();
                    } catch (AggregationException e) {
                        throw new SQLException(String.format(
                                "Date 类型转换错误：[%s]", column));
                    }

                    if (null != utilDate) {
                        sqlDate = new java.sql.Date(utilDate.getTime());
                    }
                    preparedStatement.setDate(columnIndex + 1, sqlDate);
                }
                break;

            case Types.TIME:
                if (column.getByteSize() == 0) {
                    preparedStatement.setDate(columnIndex + 1, null);
                    break;
                }
                try {
                    utilDate = column.asDate();
                } catch (AggregationException e) {
                    throw new SQLException(String.format(
                            "TIME 类型转换错误：[%s]", column));
                }
                if (null != utilDate) {
                    preparedStatement.setTimestamp(columnIndex + 1,
                            new java.sql.Timestamp(utilDate.getTime()));
                } else {
                    preparedStatement.setDate(columnIndex + 1, null);
                }
                break;
            case Types.TIMESTAMP:
                if (column.getByteSize() == 0) {
                    preparedStatement.setDate(columnIndex + 1, null);
                    break;
                }
                if (column instanceof TimestampColumn) {
                    preparedStatement.setTimestamp(columnIndex + 1, ((TimestampColumn) column).asTimestamp());
                    break;
                }
                try {
                    utilDate = column.asDate();
                } catch (AggregationException e) {
                    LOG.error("TIMESTAMP 类型转换错误：[{}]", column, e);
                    throw new SQLException(String.format(
                            "TIMESTAMP 类型转换错误：[%s]", column));
                }

                if (null != utilDate) {
                    preparedStatement.setTimestamp(columnIndex + 1,
                            new java.sql.Timestamp(utilDate.getTime()));
                } else {
                    preparedStatement.setDate(columnIndex + 1, null);
                }
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

    protected void doOneInsert(Connection connection, List<Record> buffer) {
        PreparedStatement preparedStatement = null;
        try {
            connection.setAutoCommit(true);
            preparedStatement = connection
                    .prepareStatement(this.insertSql);

            for (Record record : buffer) {
                try {
                    preparedStatement = fillPreparedStatement(
                            preparedStatement, record);
                    preparedStatement.execute();
                } catch (SQLException e) {
                    LOG.debug(e.toString());
                    // 脏数据收集
                } finally {
                    // 最后不要忘了关闭 preparedStatement
                    preparedStatement.clearParameters();
                }
            }
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
        return sourcePlugin.getConnection(dataSource);
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
