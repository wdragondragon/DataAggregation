package com.jdragon.aggregation.mysql8;

import com.jdragon.aggregation.commons.element.DoubleColumn;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.datasource.mysql8.Mysql8SourcePlugin;
import com.jdragon.aggregation.rdbms.reader.CommonRdbmsReader;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.text.DecimalFormat;

public class Mysql8Reader extends CommonRdbmsReader {

    private final DecimalFormat DOUBLE_VALUE_FORMAT = new DecimalFormat("#.###############");

    public Mysql8Reader() {
        super(new Mysql8SourcePlugin());
    }


    @Override
    protected void addRecord(ResultSet rs, ResultSetMetaData metaData, int i, Record record, String mandatoryEncoding) throws Exception {
        int columnType = metaData.getColumnType(i);
        switch (columnType) {
            case Types.REAL:
            case Types.FLOAT:
                if (StringUtils.isBlank(rs.getString(i))) {
                    record.addColumn(new DoubleColumn());
                } else {
                    Object object = rs.getObject(i);
                    String s = object.toString();
                    if (!s.contains("E")) {
                        record.addColumn(new DoubleColumn(s));
                        return;
                    }
                    String format = DOUBLE_VALUE_FORMAT.format(object);
                    record.addColumn(new DoubleColumn(format));
                }
                break;
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.DOUBLE:
                if (StringUtils.isBlank(rs.getString(i))) {
                    record.addColumn(new DoubleColumn());
                } else {
                    record.addColumn(new DoubleColumn(rs.getDouble(i)));
                }
                break;
            default:
                super.addRecord(rs, metaData, i, record, mandatoryEncoding);
                break;
        }
    }
}
