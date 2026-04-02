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
}
