package com.jdragon.aggregation.datasource.rdbms;

import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.DataSourceType;

import java.sql.Connection;

public interface RdbmsSourceDefine {
    /**
     * 获取数据库类型
     */
    DataSourceType getType();

    /**
     * 获取驱动
     *
     * @return {@link String}
     */
    String getDriver();

    /**
     * 获取sql的引号
     *
     * @return {@link String}
     */
    String getQuotationMarks();

    /**
     * 获取jdbc url规则
     *
     * @return {@link String}
     */
    String getJdbc();

    /**
     * 获取扩展参数开始符
     *
     * @return {@link String}
     */
    default String getExtraParameterStart() {
        return "?";
    }

    /**
     * 获取分隔符，jdbc参数的分隔符
     *
     * @return {@link String}
     */
    default String getSeparator() {
        return "&";
    }

    /**
     * 拼接jdbc url
     *
     * @return {@link String}
     */
    String joinJdbcUrl(BaseDataSourceDTO dataSource);


    /**
     * 获取连接
     *
     * @return {@link Connection}
     */
    Connection getConnection(BaseDataSourceDTO dataSource);


    /**
     * 获取测试链接sql
     */
    String getTestQuery();
}
