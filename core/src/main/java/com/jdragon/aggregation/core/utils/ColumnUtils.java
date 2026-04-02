package com.jdragon.aggregation.core.utils;

import com.jdragon.aggregation.commons.element.*;

import java.util.Date;

public class ColumnUtils {
    public static Column object2Column(Object o) {
        if (o == null) {
            return new StringColumn(null);
        }

        if (o instanceof String) {
            return new StringColumn((String) o);
        } else if (o instanceof Integer) {
            return new LongColumn((Integer) o);
        } else if (o instanceof Long) {
            return new LongColumn((Long) o);
        } else if (o instanceof Double) {
            return new DoubleColumn((Double) o);
        } else if (o instanceof Boolean) {
            return new BoolColumn((Boolean) o);
        } else if (o instanceof Float) {
            return new DoubleColumn((Float) o);
        } else if (o instanceof Date) {
            return new DateColumn((Date) o);
        } else if (o instanceof byte[]) {
            return new BytesColumn((byte[]) o);
        } else {
            return new ObjectColumn(o);
        }
    }

    public static Object column2Object(Column column) {
        if (column == null) {
            return null;
        }

        if (column instanceof StringColumn || column instanceof DateColumn) {
            return column.asString();
        } else if (column instanceof LongColumn) {
            return column.asLong();
        } else if (column instanceof DoubleColumn) {
            return column.asDouble();
        } else if (column instanceof BoolColumn) {
            return column.asBoolean();
        } else if (column instanceof BytesColumn) {
            return column.asBytes();
        } else if (column instanceof ObjectColumn) {
            return column.asString();
        }
        return column.getRawData();
    }
}
