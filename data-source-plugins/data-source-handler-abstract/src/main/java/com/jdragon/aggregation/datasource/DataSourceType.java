package com.jdragon.aggregation.datasource;

import lombok.Getter;

@Getter
public enum DataSourceType {
    MySql("mysql", "com.mysql.jdbc.Driver"),
    Mysql8("mysql8", "com.mysql.cj.jdbc.Driver"),
    Oracle("oracle", "oracle.jdbc.OracleDriver"),
    SQLServer("sqlserver", "com.microsoft.sqlserver.jdbc.SQLServerDriver"),
    PostgreSQL("postgresql", "org.postgresql.Driver"),
    DM("dm", "dm.jdbc.driver.DmDriver"),
    HIVE("hive", "org.apache.hive.jdbc.HiveDriver"),
    ClickHouse("clickhouse", "ru.yandex.clickhouse.ClickHouseDriver"),

    ;
    private final String typeName;

    private final String driverClassName;

    DataSourceType(String typeName, String driverClassName) {
        this.typeName = typeName;
        this.driverClassName = driverClassName;
    }
}
