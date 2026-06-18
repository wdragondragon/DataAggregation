package com.jdragon.aggregation.oracle;

import com.jdragon.aggregation.commons.element.DoubleColumn;
import com.jdragon.aggregation.commons.element.LongColumn;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.datasource.oracle.OracleSourcePlugin;
import com.jdragon.aggregation.rdbms.reader.CommonRdbmsReader;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

public class OracleReader extends CommonRdbmsReader {

    public OracleReader() {
        super(new OracleSourcePlugin());
    }

    @Override
    protected void addRecord(ResultSet rs, ResultSetMetaData metaData, int i, Record record, String mandatoryEncoding) throws Exception {
        int columnType = metaData.getColumnType(i);
        switch (columnType) {
            case Types.NUMERIC:
            case Types.DECIMAL:
                // Oracle NUMBER has scale=0 for integers, scale>0 for decimals.
                // Use LongColumn for integers to avoid Double precision loss on large numbers.
                if (StringUtils.isBlank(rs.getString(i))) {
                    record.addColumn(new DoubleColumn());
                } else {
                    int scale = metaData.getScale(i);
                    if (scale == 0) {
                        record.addColumn(new LongColumn(rs.getString(i)));
                    } else {
                        record.addColumn(new DoubleColumn(rs.getString(i)));
                    }
                }
                break;
            default:
                super.addRecord(rs, metaData, i, record, mandatoryEncoding);
                break;
        }
    }
}
