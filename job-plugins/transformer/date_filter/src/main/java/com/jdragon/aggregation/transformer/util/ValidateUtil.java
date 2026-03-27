package com.jdragon.aggregation.transformer.util;

import com.jdragon.aggregation.commons.element.*;

import java.sql.Date;

/**
 * <p>
 * <p>
 * </p>
 *
 * @author jdragon
 * @since 2026/3/27
 */
public class ValidateUtil {

    /**
     * 根据字段类型生成相应的字段
     * @param column
     * @param value
     * @param record
     */
    public static Record validate(Column column, String value, Record record,Integer columnIndex){
        if (column.getType() == Column.Type.STRING) {
            record.setColumn(columnIndex, new StringColumn(value));
        } else if (column.getType() == Column.Type.DATE) {
            record.setColumn(columnIndex, new DateColumn(Date.valueOf(value)));
        } else if (column.getType() == Column.Type.LONG || column.getType() == Column.Type.INT) {
            Long newValue = Long.valueOf(value);
            record.setColumn(columnIndex, new LongColumn(newValue));
        } else if (column.getType() == Column.Type.BOOL) {
            Boolean newValue = Boolean.valueOf(value);
            record.setColumn(columnIndex, new BoolColumn(newValue));
        } else if (column.getType() == Column.Type.DOUBLE) {
            Double newValue = Double.valueOf(value);
            record.setColumn(columnIndex, new DoubleColumn(newValue));
        }
        return record;
    }
}
