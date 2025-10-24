package com.jdragon.aggregation.datasource.file.tbds.hdfs;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.*;
import java.time.format.DateTimeFormatter;

public class Int96Converter {

    private static final int JULIAN_EPOCH_OFFSET_DAYS = 2440588; // Julian 1970-01-01
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * 将 Parquet INT96 12字节时间戳转换为 yyyy-MM-dd HH:mm:ss 字符串
     *
     * @param bytes 12字节 INT96
     * @return 格式化后的时间字符串
     */
    public static String int96ToString(byte[] bytes) {
        if (bytes == null || bytes.length != 12) {
            throw new IllegalArgumentException("INT96 timestamp must be 12 bytes");
        }

        // 小端解析
        ByteBuffer buf = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        long nanosOfDay = buf.getLong();  // 前8字节纳秒
        int julianDays = buf.getInt();    // 后4字节Julian天数

        // 转成 LocalDate
        int daysSinceEpoch = julianDays - JULIAN_EPOCH_OFFSET_DAYS;
        LocalDate date = LocalDate.ofEpochDay(daysSinceEpoch);

        // 转成 LocalTime
        long seconds = nanosOfDay / 1_000_000_000L;
        LocalTime time = LocalTime.ofSecondOfDay(seconds);

        LocalDateTime ldt = LocalDateTime.of(date, time);

        // 格式化
        return ldt.format(FORMATTER);
    }


    // 常量定义
    private static final long JULIAN_DAY_OF_EPOCH = 2440588; // 1970-01-01的儒略日
    private static final long MILLIS_IN_DAY = 86400000; // 一天的毫秒数

    public static LocalDateTime convertInt96ToDateTime(byte[] bytes) {
        if (bytes == null || bytes.length != 12) {
            throw new IllegalArgumentException("INT96 timestamp must be exactly 12 bytes");
        }

        // 使用ByteBuffer处理字节顺序
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

        // 读取纳秒部分（前8字节）
        long timeOfDayNanos = buffer.getLong();

        // 读取儒略日（后4字节）
        int julianDay = buffer.getInt();

        // 计算Unix时间戳（毫秒）
        long unixTimeMillis = (julianDay - JULIAN_DAY_OF_EPOCH) * MILLIS_IN_DAY;
        unixTimeMillis += timeOfDayNanos / 1_000_000; // 纳秒转毫秒

        // 转换为LocalDateTime
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(unixTimeMillis), ZoneId.systemDefault());
    }

    public static void main(String[] args) {
        // 示例数据
        byte[] timestampBytes = {0, 0, -118, 11, 99, 52, 0, 0, 15, -115, 37, 0};

        // 转换并格式化输出
        LocalDateTime dateTime = convertInt96ToDateTime(timestampBytes);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        System.out.println("Converted timestamp: " + dateTime.format(formatter));
    }
}
