package com.jdragon.aggregation.commons.element;

import java.sql.Timestamp;

/**
 * @author JDragon
 * @date 2023/7/27 17:29
 * @description
 */
public class TimestampColumn extends DateColumn {

    private final Timestamp timestamp;

    /**
     * 构建值为stamp(Unix时间戳)的DateColumn，使用Date子类型为DATETIME
     * 实际存储有date改为long的ms，节省存储
     */
    public TimestampColumn(final Timestamp timestamp) {
        super(timestamp);
        this.timestamp = timestamp;
    }

    public Timestamp asTimestamp() {
        return timestamp;
    }

    @Override
    public Column clone() throws CloneNotSupportedException {
        return new TimestampColumn(timestamp);
    }
}
