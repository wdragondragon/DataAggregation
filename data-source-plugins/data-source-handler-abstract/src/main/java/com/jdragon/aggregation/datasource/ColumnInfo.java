package com.jdragon.aggregation.datasource;

import lombok.Data;

@Data
public class ColumnInfo {
    /**
     * 表所在的 catalog（数据库名）
     */
    private String tableCat;

    /**
     * 表的 schema 名（MySQL 中通常等同于数据库名）
     */
    private String tableSchem;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 列名（字段名）
     */
    private String columnName;

    /**
     * SQL 类型代码（对应 java.sql.Types）
     */
    private int dataType;

    /**
     * 数据库中数据类型名称（如 VARCHAR、INT）
     */
    private String typeName;

    /**
     * 列的大小，例如 VARCHAR(255) 的 255
     */
    private int columnSize;

    /**
     * 小数点位数（用于浮点型）
     */
    private int decimalDigits;

    /**
     * 精度基数，通常为 10 或 2
     */
    private int numPrecRadix;

    /**
     * 是否允许为 NULL（0=不允许，1=允许，2=未知）
     */
    private int nullable;

    /**
     * 字段注释（说明）
     */
    private String remarks;

    /**
     * 默认值
     */
    private String columnDef;

    /**
     * 对于字符串类型，最大字节长度
     */
    private int charOctetLength;

    /**
     * 字段在表中的位置（从 1 开始）
     */
    private int ordinalPosition;

    /**
     * 是否允许为 NULL（"YES" 或 "NO"）
     */
    private String isNullable;

    /**
     * 是否自动递增（"YES"、"NO"）
     */
    private String isAutoincrement;

    /**
     * 是否是计算生成的字段（"YES" 或 "NO"）
     */
    private String isGeneratedColumn;

    /**
     * 是否为主键（"YES" 或 "NO"）
     */
    private String isPrimaryKey;
}