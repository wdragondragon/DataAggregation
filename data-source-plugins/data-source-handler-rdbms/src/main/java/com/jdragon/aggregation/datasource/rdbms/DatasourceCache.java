package com.jdragon.aggregation.datasource.rdbms;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DatasourceCache {

    private final static Cache<String, DataSource> dataSourceCache = Caffeine.newBuilder()
            .removalListener((String key, DataSource dataSource, RemovalCause cause) -> {
                try {
                    log.info("close datasource key:{}", key);
                    if (dataSource instanceof HikariDataSource) {
                        ((HikariDataSource) dataSource).close();
                    }
                } catch (Exception e) {
                    log.warn("close datasource error", e);
                }
            }).expireAfterAccess(60, TimeUnit.MINUTES).build();

    public static Connection getConnection(String jdbcUrl, String driverClassName, String user, String password, String testQuery) throws SQLException {
        return get(jdbcUrl, driverClassName, user, password, testQuery).getConnection();
    }

    public static DataSource get(String jdbcUrl, String driverClassName, String user, String password, String testQuery) {
        String key = String.join("*",
                jdbcUrl,
                driverClassName,
                user,
                password);
        try {
            DataSource dataSource;
            dataSource = dataSourceCache.getIfPresent(key);
            if (dataSource != null) {
                return dataSource;
            }
            HikariConfig hikariConfig = new HikariConfig();
            hikariConfig.setPoolName(key);
            hikariConfig.setJdbcUrl(jdbcUrl);
            hikariConfig.setUsername(user);
            hikariConfig.setPassword(password);
            hikariConfig.setDriverClassName(driverClassName);
            hikariConfig.setMaximumPoolSize(20);
            hikariConfig.setMinimumIdle(2);
            hikariConfig.setIdleTimeout(60000L);
            hikariConfig.setMaxLifetime(180000L);
            hikariConfig.setKeepaliveTime(30000L);
            hikariConfig.setConnectionTimeout(60000L);
            if (StringUtils.isNotBlank(testQuery)) {
                hikariConfig.setConnectionTestQuery(testQuery);
            }
            HikariDataSource hikariDataSource = new HikariDataSource(hikariConfig);
            dataSourceCache.put(key, hikariDataSource);
            return hikariDataSource;
        } catch (Throwable e) {
            throw e;
        }
    }

}