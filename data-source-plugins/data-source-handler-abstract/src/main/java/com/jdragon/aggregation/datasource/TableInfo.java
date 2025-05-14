package com.jdragon.aggregation.datasource;

import lombok.Data;

@Data
public class TableInfo {
    /**
     * 数据库名（catalog）
     */
    private String tableCat;

    /**
     * schema 名（MySQL 中为数据库名）
     */
    private String tableSchem;

    /**
     * 表名
     */
    private String tableName;

    /**
     * 表类型（如 TABLE、VIEW）
     */
    private String tableType;

    /**
     * 表注释
     */
    private String remarks;

    /**
     * 类型 catalog（一般为 null）
     */
    private String typeCat;

    /**
     * 类型 schema（一般为 null）
     */
    private String typeSchem;

    /**
     * 类型名称（一般为 null）
     */
    private String typeName;

    /**
     * 自引用列名（若有）
     */
    private String selfReferencingColName;

    /**
     * 引用生成方式（如 SYSTEM）
     */
    private String refGeneration;

    /**
     * 是否外部表
     */
    private boolean externalTable;
}
