package com.jdrdagon.aggregation.postgresql;

import com.jdragon.aggregation.commons.element.DoubleColumn;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.element.TimestampColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.postgres.PGSourcePlugin;
import com.jdragon.aggregation.rdbms.reader.CommonRdbmsReader;
import com.jdragon.aggregation.rdbms.util.DBUtilErrorCode;
import org.postgresql.util.PGInterval;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

public class PostgresqlReader extends CommonRdbmsReader {

    public PostgresqlReader() {
        super(new PGSourcePlugin());
    }

    @Override
    public void init() {
        Configuration originalConfig = super.getPluginJobConf();
        int fetchSize = originalConfig.getInt(com.jdragon.aggregation.rdbms.reader.Constant.FETCH_SIZE,
                Constant.DEFAULT_FETCH_SIZE);
        if (fetchSize < 1) {
            throw AggregationException.asException(DBUtilErrorCode.REQUIRED_VALUE,
                    String.format("您配置的fetchSize有误，根据引擎的设计，fetchSize : [%d] 设置值不能小于 1.", fetchSize));
        }
        originalConfig.set(com.jdragon.aggregation.rdbms.reader.Constant.FETCH_SIZE, fetchSize);
        super.init();
    }

    @Override
    protected void addRecord(ResultSet rs, ResultSetMetaData metaData, int i, Record record, String mandatoryEncoding) throws Exception {
        int columnType = metaData.getColumnType(i);
        switch (columnType) {
            case Types.TIMESTAMP:
                record.addColumn(new TimestampColumn(rs.getTimestamp(i)));
                break;
            case Types.REAL:
                record.addColumn(new DoubleColumn(rs.getFloat(i)));
                break;
            case Types.OTHER:
                Object object = rs.getObject(i);
                if (object instanceof PGInterval) {
                    record.addColumn(new StringColumn(rs.getString(i)));
                } else {
                    super.addRecord(rs, metaData, i, record, mandatoryEncoding);
                }
                break;
            default:
                super.addRecord(rs, metaData, i, record, mandatoryEncoding);
                break;
        }
    }
}
