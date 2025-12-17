package com.jdragon.aggregation.datasource;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class BaseDataSourceDTO {
    /**
     * 数据源名称
     */
    private String name;

    /**
     * 数据源类型
     */
    private String type;

    /**
     * 主机IP
     */
    private String host;
    /**
     * 端口号
     */
    private String port;
    /**
     * 数据库名
     */
    private String database;

    /**
     * 用户名
     */
    private String userName;
    /**
     * 密码
     */
    private String password;

    /**
     * 数据库连接参数，eg:useSSL=false, serverTimezone=UTC, socketTimeout=1000
     */
    private String other;

    private boolean usePool;

    /**
     * 额外参数
     * {@link com.jdragon.aggregation.datasource.params.DataSourceExtraParamKey}
     */
    private Map<String, String> extraParams = new HashMap<>();

    /**
     * 主要用于 influxdb，类似schema
     */
    private String bucket;

    private String principal;

    private String keytabPath;

    private String krb5File;

    private String jdbcUrl;

    private String driverClassName;
}
