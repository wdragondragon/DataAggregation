package com.jdragon.aggregation.writer.oracle;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.element.TimestampColumn;
import com.jdragon.aggregation.datasource.oracle.OracleSourcePlugin;
import com.jdragon.aggregation.rdbms.writer.CommonRdbmsWriter;

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

public class OracleWriter extends CommonRdbmsWriter {

    public OracleWriter() {
        super(new OracleSourcePlugin());
    }

    @Override
    protected PreparedStatement fillPreparedStatementColumnType(
            PreparedStatement preparedStatement, int columnIndex, int columnSqltype, Column column) throws SQLException {

        switch (columnSqltype) {
            case Types.CLOB:
                // Oracle CLOB requires character stream, not binary stream
                if (null == column.getRawData()) {
                    preparedStatement.setObject(columnIndex + 1, null);
                } else {
                    String clobData = column.asString();
                    preparedStatement.setCharacterStream(columnIndex + 1,
                            new StringReader(clobData), clobData.length());
                }
                break;

            case Types.NUMERIC:
            case Types.DECIMAL:
                if (emptyAsNull && "".equals(column.asString())) {
                    preparedStatement.setString(columnIndex + 1, null);
                } else if (null == column.getRawData()) {
                    preparedStatement.setObject(columnIndex + 1, null);
                } else {
                    preparedStatement.setBigDecimal(columnIndex + 1, column.asBigDecimal());
                }
                break;

            case Types.TIMESTAMP:
                if (column.getByteSize() == 0) {
                    preparedStatement.setDate(columnIndex + 1, null);
                    break;
                }
                if (column instanceof TimestampColumn) {
                    preparedStatement.setTimestamp(columnIndex + 1,
                            ((TimestampColumn) column).asTimestamp());
                    break;
                }
                if (column instanceof StringColumn) {
                    preparedStatement.setString(columnIndex + 1, column.asString());
                    break;
                }
                Date date = column.asDate();
                if (null != date) {
                    preparedStatement.setTimestamp(columnIndex + 1,
                            new Timestamp(date.getTime()));
                } else {
                    preparedStatement.setDate(columnIndex + 1, null);
                }
                break;

            default:
                return super.fillPreparedStatementColumnType(
                        preparedStatement, columnIndex, columnSqltype, column);
        }
        return preparedStatement;
    }
}
