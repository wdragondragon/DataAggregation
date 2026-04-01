package com.jdragon.aggregation.writer.postgresql;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.element.TimestampColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.datasource.postgres.PGSourcePlugin;
import com.jdragon.aggregation.rdbms.writer.CommonRdbmsWriter;
import com.jdragon.aggregation.rdbms.writer.Constant;
import com.jdragon.aggregation.writer.postgresql.task.PostgresqlBulkTextTask;

import java.sql.*;
import java.util.Date;

public class PostgresqlWriter extends CommonRdbmsWriter {

    private PostgresqlBulkTextTask bulkTextTask = null;

    private String writeMode;

    public PostgresqlWriter() {
        super(new PGSourcePlugin());
    }


    @Override
    public void init() {
        Configuration writerSliceConfig = super.getPluginJobConf();
        writeMode = writerSliceConfig.getString("writeMode");
        if (Constant.COPY.equals(writeMode)) {
            bulkTextTask = new PostgresqlBulkTextTask();
            bulkTextTask.init();
        } else {
            super.init();
        }
    }


    @Override
    public void prepare() {
        if (Constant.COPY.equals(writeMode)) {
            bulkTextTask.prepare();
        } else {
            super.prepare();
        }
    }

    @Override
    public void startWrite(RecordReceiver recordReceiver) {
        if (Constant.COPY.equals(writeMode)) {
            bulkTextTask.startWrite(recordReceiver);
        } else {
            super.startWrite(recordReceiver);
        }
    }

    @Override
    public void post() {
        if (Constant.COPY.equals(writeMode)) {
            bulkTextTask.post();
        } else {
            super.post();
        }
    }

    @Override
    public void destroy() {
        if (Constant.COPY.equals(writeMode)) {
            bulkTextTask.destroy();
        } else {
            super.destroy();
        }
    }

    @Override
    public String calcValueHolder(String columnType) {
        if ("serial".equalsIgnoreCase(columnType)) {
            return "?::int";
        } else if ("bigserial".equalsIgnoreCase(columnType)) {
            return "?::int8";
        } else if ("bit".equalsIgnoreCase(columnType)) {
            return "?::bit varying";
        }
        return "?::" + columnType;
    }

    @Override
    protected PreparedStatement fillPreparedStatementColumnType(PreparedStatement preparedStatement, int columnIndex, int columnSqltype, Column column) throws SQLException {
        if (columnSqltype == Types.TIMESTAMP) {
            if (column.getByteSize() == 0) {
                preparedStatement.setDate(columnIndex + 1, null);
                return preparedStatement;
            }
            if (column instanceof StringColumn) {
                preparedStatement.setString(columnIndex + 1, column.asString());
                return preparedStatement;
            }
            if (column instanceof TimestampColumn) {
                preparedStatement.setTimestamp(columnIndex + 1, ((TimestampColumn) column).asTimestamp());
                return preparedStatement;
            } else {
                Date date = column.asDate();
                preparedStatement.setTimestamp(columnIndex + 1, new Timestamp(date.getTime()));
                return preparedStatement;
            }
        }
        return super.fillPreparedStatementColumnType(preparedStatement, columnIndex, columnSqltype, column);
    }
}
