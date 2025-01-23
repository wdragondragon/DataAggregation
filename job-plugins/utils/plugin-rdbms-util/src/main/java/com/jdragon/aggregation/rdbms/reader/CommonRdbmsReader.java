package com.jdragon.aggregation.rdbms.reader;

import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.element.*;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.rdbms.RdbmsSourcePlugin;
import com.jdragon.aggregation.rdbms.util.DBUtilErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.sql.*;
import java.util.Date;
import java.util.List;

public class CommonRdbmsReader extends Reader.Job {

    private static final Logger LOG = LoggerFactory.getLogger(CommonRdbmsReader.class);

    private static final boolean IS_DEBUG = LOG.isDebugEnabled();

    private BaseDataSourceDTO dataSource;

    private final RdbmsSourcePlugin sourcePlugin;

    private String mandatoryEncoding;

    private String selectSql;

    public CommonRdbmsReader(RdbmsSourcePlugin sourcePlugin) {
        this.sourcePlugin = sourcePlugin;
    }

    @Override
    public void init() {
        Configuration connectConfig = this.getPluginJobConf().getConfiguration("connect");
        dataSource = JSONObject.parseObject(connectConfig.toString(), BaseDataSourceDTO.class);
        dataSource.setName(sourcePlugin.getType().getTypeName());
        String tableName = this.getPluginJobConf().getString("table");
        List<String> columns = this.getPluginJobConf().getList("columns", String.class);
        selectSql = String.format("select %s from %s", String.join(",", columns), tableName);
        LOG.info("rdbms query sql: {}", selectSql);
        mandatoryEncoding = this.getPluginJobConf().getString("mandatoryEncoding", "utf-8");
    }

    @Override
    public void prepare() {
        sourcePlugin.executeQuerySql(dataSource, "select 1", true);
    }


    @Override
    public void startRead(RecordSender recordSender) {
        try {
            Connection connection = sourcePlugin.getConnection(dataSource);
            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(selectSql);
            ResultSetMetaData metaData = rs.getMetaData();
            int columnNumber = metaData.getColumnCount();
            while (rs.next()) {
                this.transportOneRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding);
            }
            rs.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected Record transportOneRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding) {
        Record record = buildRecord(recordSender, rs, metaData, columnNumber, mandatoryEncoding);
        recordSender.sendToWriter(record);
        return record;
    }

    protected Record buildRecord(RecordSender recordSender, ResultSet rs, ResultSetMetaData metaData, int columnNumber, String mandatoryEncoding) {
        Record record = recordSender.createRecord();

        try {
            for (int i = 1; i <= columnNumber; i++) {
                addRecord(rs, metaData, i, record, mandatoryEncoding);
            }
        } catch (Exception e) {
            if (IS_DEBUG) {
                LOG.debug("read data {} occur exception:", record.toString(), e);
            }
            //TODO 这里识别为脏数据靠谱吗？
            if (e instanceof AggregationException) {
                throw (AggregationException) e;
            }
        }
        return record;
    }


    protected void addRecord(ResultSet rs, ResultSetMetaData metaData, int i, Record record, String mandatoryEncoding) throws Exception {
        int columnType = metaData.getColumnType(i);
        switch (columnType) {
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.SQLXML:
                String rawData;
                if (StringUtils.isBlank(mandatoryEncoding)) {
                    rawData = rs.getString(i);
                } else {
                    rawData = rs.getBytes(i) == null ? null : new String(rs.getBytes(i), mandatoryEncoding);
                }
                if (rawData == null) {
                    record.addColumn(new StringColumn());
                } else {
                    record.addColumn(new StringColumn(rawData));
                }
                break;

            case Types.CLOB:
            case Types.NCLOB:
                Clob clob;
                if (columnType == Types.CLOB) {
                    clob = rs.getClob(i);
                } else {
                    clob = rs.getNClob(i);
                }
                if (clob == null) {
                    record.addColumn(new StringColumn());
                    break;
                }
                StringBuilder builder = new StringBuilder();
                java.io.Reader reader = clob.getCharacterStream();
                BufferedReader br = new BufferedReader(reader);

                String line;
                while (null != (line = br.readLine())) {
                    builder.append(line);
                }
                record.addColumn(new StringColumn(builder.toString()));
                break;
            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.INTEGER:
            case Types.BIGINT:
                if (StringUtils.isBlank(rs.getString(i))) {
                    record.addColumn(new LongColumn());
                } else {
                    record.addColumn(new LongColumn(rs.getString(i)));
                }
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                if (StringUtils.isBlank(rs.getString(i))) {
                    record.addColumn(new DoubleColumn());
                } else {
                    record.addColumn(new DoubleColumn(rs.getString(i)));
                }
                break;
            case Types.TIME:
                if (StringUtils.isBlank(rs.getString(i))) {
                    record.addColumn(new DateColumn());
                } else {
                    DateColumn dateColumn = new DateColumn(rs.getTime(i));
                    record.addColumn(new DateColumn(rs.getTime(i)));
                }
                break;
            // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
            case Types.DATE:
                if (metaData.getColumnTypeName(i).equalsIgnoreCase("year")) {
                    if (StringUtils.isBlank(rs.getString(i))) {
                        record.addColumn(new LongColumn());
                    } else {
                        record.addColumn(new LongColumn(rs.getInt(i)));
                    }
                } else {
                    if (StringUtils.isBlank(rs.getString(i))) {
                        record.addColumn(new DateColumn());
                    } else {
                        java.sql.Date date = rs.getDate(i);
                        String s = date.toString();
                        String s1 = new DateColumn(rs.getDate(i)).asString();
                        DateColumn dateColumn = new DateColumn(rs.getDate(i));
                        if (!s.equals(s1)) {
                            long time = date.getTime();
                            DateColumn temp = new DateColumn(new java.sql.Date(time + (1000 * 60 * 60)));
                            if (temp.asString().equals(s)) {
                                dateColumn = temp;
                            }
                        }
                        record.addColumn(dateColumn);
                    }
                }
                break;
            case Types.TIMESTAMP:
                if (rs.getString(i) == null || "".equals(rs.getString(i))) {
                    record.addColumn(new DateColumn((Date) null));
                } else {
                    record.addColumn(new TimestampColumn(rs.getTimestamp(i)));
                }
                break;
            case -101:
            case -102:
                if (rs.getString(i) == null || rs.getString(i).isEmpty()) {
                    Date date = null;
                    record.addColumn(new DateColumn(date));
                } else {
                    record.addColumn(new DateColumn(new StringColumn(String.valueOf(rs.getString(i))).asDate()));
                }
                break;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                record.addColumn(new BytesColumn(rs.getBytes(i)));
                break;
            case Types.BLOB:
                Blob blob = rs.getBlob(i);
                byte[] bytes = blob.getBytes(0, (int) blob.length());
                record.addColumn(new BytesColumn(bytes));
                break;
            case Types.OTHER:
                record.addColumn(new ObjectColumn(rs.getObject(i)));
                break;
            case Types.BOOLEAN:
            case Types.BIT:
                if (StringUtils.isBlank(rs.getString(i))) {
                    record.addColumn(new BoolColumn());
                } else {
                    record.addColumn(new BoolColumn(rs.getBoolean(i)));
                }
                break;

            case Types.NULL:
                String stringData = null;
                if (rs.getObject(i) != null) {
                    stringData = rs.getObject(i).toString();
                }
                record.addColumn(new StringColumn(stringData));
                break;
            default:
                throw AggregationException.asException(DBUtilErrorCode.UNSUPPORTED_TYPE, String.format("您的配置文件中的列配置信息有误. 因为引擎不支持数据库读取这种字段类型. Types:[%s], 字段名:[%s], 字段名称:[%s], 字段Java类型:[%s]. 请尝试使用数据库函数将其转换引擎支持的类型 或者不同步该字段 .", columnType, metaData.getColumnName(i), metaData.getColumnType(i), metaData.getColumnClassName(i)));
        }
    }

    @Override
    public void post() {
        // do nothing
    }

    @Override
    public void destroy() {
        sourcePlugin.destroy();
    }
}
