package com.jdragon.aggregation.writer.postgresql.task;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.datasource.postgres.PGSourcePlugin;
import com.jdragon.aggregation.rdbms.util.DBUtilErrorCode;
import com.jdragon.aggregation.rdbms.util.Key;
import com.jdragon.aggregation.rdbms.writer.CommonRdbmsWriter;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.Encoding;
import org.postgresql.jdbc.PgConnection;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lht
 * @date 2022/1/13
 */
public class PostgresqlBulkTextTask extends CommonRdbmsWriter {
    /**
     * 特殊字符映射
     */
    public static final Map<Character, String> SPECIAL_CHARSETS_MAP = new HashMap<>(8, 1f);

    static {
        SPECIAL_CHARSETS_MAP.put('\t', "\\t");
        SPECIAL_CHARSETS_MAP.put('\n', "\\n");
        SPECIAL_CHARSETS_MAP.put('\r', "\\r");
        SPECIAL_CHARSETS_MAP.put('\\', "\\\\");
    }

    public PostgresqlBulkTextTask() {
        super(new PGSourcePlugin());
    }


    @Override
    public void init() {
        super.init();
        emptyAsNull = getPluginJobConf().getBool(Key.EMPTY_AS_NULL, true);
        String BASIC_MESSAGE = String.format("jdbcUrl:[%s], table:[%s]", this.getDataSource().getJdbcUrl(), this.getTableName());
        LOG.info("Configuration: emptyAsNull: [{}], {}", emptyAsNull, BASIC_MESSAGE);
    }


    @Override
    protected void doBatchInsert(Connection connection, List<Record> buffer) throws SQLException {
        try {
            connection.setAutoCommit(false);
            CopyManager copyManager = ((PgConnection) connection).getCopyAPI();
            CopyIn copyIn = copyManager.copyIn(this.getInsertSql());
            byte[] copyBytes = getCopyBytes(buffer, ((PgConnection) connection).getEncoding());
            copyIn.writeToCopy(copyBytes, 0, copyBytes.length);
            copyIn.flushCopy();
            copyIn.endCopy();
            connection.commit();
        } catch (SQLException e) {
            LOG.warn("回滚此次写入, 采用每次写入一行方式提交. 执行的sql为: " + this.getInsertSql() + ", 因为:" + e.getMessage());
            connection.rollback();
            doOneInsert(connection, buffer);
        } catch (Exception e) {
            throw AggregationException.asException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }
    }

    /**
     * 将records 转换为byte数组， 使用PgConnection的Encoding进行编码
     *
     * @return byte[]
     * @author lht
     * @date 2022/1/17
     **/
    protected byte[] getCopyBytes(List<Record> records, Encoding encoding) throws IOException {
        StringBuilder builder = new StringBuilder();
        for (Record record : records) {
            builder.append(handleSpecialCharsets(record.getColumn(0).asString()));
            for (int i = 1; i < this.getColumnNumber(); i++) {
                builder.append("\t");
                builder.append(handleSpecialCharsets(record.getColumn(i).asString()));
            }
            builder.append("\n");
        }
        return encoding.encode(builder.toString());
    }

    /**
     * 用于映射NULL值 null -> \\N, 和特殊字符处理并且用于支持{@code emptyAsNull} 属性
     *
     * @return java.lang.String
     * @author lht
     * @date 2022/1/17
     **/
    protected String handleSpecialCharsets(String value) {
        if (null == value) {
            return "\\N";
        }
        if (emptyAsNull && StringUtils.isEmpty(value)) {
            return "\\N";
        }
        // Add 10% for escaping;
        StringBuilder builder = new StringBuilder(2 + (value.length() + 10) / 10 * 11);
        char[] chars = value.toCharArray();
        for (char achar : chars) {
            String replaceChar = SPECIAL_CHARSETS_MAP.getOrDefault(achar, String.valueOf(achar));
            builder.append(replaceChar);
        }
        return builder.toString();
    }

    @Override
    protected void doOneInsert(Connection connection, List<Record> buffer) {
        try {
            connection.setAutoCommit(false);
            CopyManager copyManager = ((PGConnection) connection).getCopyAPI();
            CopyIn copyIn;
            for (Record record : buffer) {
                byte[] copyBytes = getCopyBytes(Collections.singletonList(record), ((PgConnection) connection).getEncoding());
                copyIn = copyManager.copyIn(this.getInsertSql());
                try {
                    copyIn.writeToCopy(copyBytes, 0, copyBytes.length);
                    copyIn.flushCopy();
                    copyIn.endCopy();
                    connection.commit();
                } catch (SQLException e) {
                    LOG.debug(e.toString());
                    connection.rollback();
                    this.getTaskPluginCollector().collectDirtyRecord(record, e);
                }
            }
        } catch (Exception e) {
            throw AggregationException.asException(
                    DBUtilErrorCode.WRITE_DATA_ERROR, e);
        }
    }
}
