package com.jdragon.aggregation.datasource.rdbms;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.jdragon.aggregation.datasource.DataSourcePoolConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class DatasourceCache {

    private final static Cache<String, DataSource> dataSourceCache = Caffeine.newBuilder()
            .removalListener((String key, DataSource dataSource, RemovalCause cause) -> {
                try {
                    String poolName = dataSource instanceof HikariDataSource
                            ? ((HikariDataSource) dataSource).getPoolName() : "non-hikari";
                    log.info("close datasource pool={}, cause={}", poolName, cause);
                    if (dataSource instanceof HikariDataSource) {
                        ((HikariDataSource) dataSource).close();
                    }
                } catch (Exception e) {
                    log.warn("close datasource error", e);
                }
            }).expireAfterAccess(60, TimeUnit.MINUTES).build();

    public static Connection getConnection(String jdbcUrl, String driverClassName, String user, String password, String testQuery) throws SQLException {
        return getConnection(jdbcUrl, driverClassName, user, password, testQuery, Collections.<String, String>emptyMap());
    }

    public static Connection getConnection(String jdbcUrl,
                                           String driverClassName,
                                           String user,
                                           String password,
                                           String testQuery,
                                           Map<String, ?> extraParams) throws SQLException {
        logMon(jdbcUrl, driverClassName, user, password, extraParams);
        Connection connection = get(jdbcUrl, driverClassName, user, password, testQuery, extraParams).getConnection();
        logMon(jdbcUrl, driverClassName, user, password, extraParams);
        return connection;
    }

    public static DataSource get(String jdbcUrl, String driverClassName, String user, String password, String testQuery) {
        return get(jdbcUrl, driverClassName, user, password, testQuery, Collections.<String, String>emptyMap());
    }

    public static DataSource get(String jdbcUrl,
                                 String driverClassName,
                                 String user,
                                 String password,
                                 String testQuery,
                                 Map<String, ?> extraParams) {
        DataSourcePoolConfig poolConfig = DataSourcePoolConfig.from(extraParams);
        String key = cacheKey(jdbcUrl, driverClassName, user, password, poolConfig);
        try {
            return dataSourceCache.get(key, cacheKey -> createDataSource(cacheKey, jdbcUrl, driverClassName, user, password, testQuery, poolConfig));
        } catch (Throwable e) {
            throw e;
        }
    }

    /**
     * Evicts every pooled variant for one datasource identity. Direct JDBC
     * mode calls this before opening a connection so a pool created by an
     * earlier configuration cannot keep consuming database connections after
     * an operator disables pooling.
     */
    public static void invalidate(String jdbcUrl,
                                  String driverClassName,
                                  String user,
                                  String password) {
        String baseKey = String.join("*", jdbcUrl, driverClassName, user, password);
        for (String key : dataSourceCache.asMap().keySet()) {
            if (baseKey.equals(key) || key.startsWith(baseKey + "|pool=")) {
                dataSourceCache.invalidate(key);
            }
        }
        dataSourceCache.cleanUp();
    }

    private static String cacheKey(String jdbcUrl,
                                   String driverClassName,
                                   String user,
                                   String password,
                                   DataSourcePoolConfig poolConfig) {
        String baseKey = String.join("*",
                jdbcUrl,
                driverClassName,
                user,
                password);
        // Preserve the legacy key for the default configuration so existing
        // callers that invalidate the old cache entry remain compatible.
        return poolConfig.isDefault() ? baseKey : baseKey + "|pool=" + poolConfig.cacheKey();
    }

    private static DataSource createDataSource(String key,
                                               String jdbcUrl,
                                               String driverClassName,
                                               String user,
                                               String password,
                                               String testQuery,
                                               DataSourcePoolConfig poolConfig) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setPoolName(poolName(key));
        hikariConfig.setJdbcUrl(jdbcUrl);
        hikariConfig.setUsername(user);
        hikariConfig.setPassword(password);
        hikariConfig.setDriverClassName(driverClassName);
        hikariConfig.setMaximumPoolSize(poolConfig.getMaximumPoolSize());
        hikariConfig.setMinimumIdle(poolConfig.getMinimumIdle());
        hikariConfig.setIdleTimeout(poolConfig.getIdleTimeoutMs());
        hikariConfig.setMaxLifetime(poolConfig.getMaxLifetimeMs());
        hikariConfig.setKeepaliveTime(poolConfig.getKeepaliveTimeMs());
        hikariConfig.setConnectionTimeout(poolConfig.getConnectionTimeoutMs());
//            hikariConfig.setRegisterMbeans(true);
        if (StringUtils.isNotBlank(testQuery)) {
            hikariConfig.setConnectionTestQuery(testQuery);
        }
        return new HikariDataSource(hikariConfig);
    }

    public static void logMon(String jdbcUrl, String driverClassName, String user, String password) {
        logMon(jdbcUrl, driverClassName, user, password, Collections.<String, String>emptyMap());
    }

    public static void logMon(String jdbcUrl,
                              String driverClassName,
                              String user,
                              String password,
                              Map<String, ?> extraParams) {
        DataSourcePoolConfig poolConfig = DataSourcePoolConfig.from(extraParams);
        String key = cacheKey(jdbcUrl, driverClassName, user, password, poolConfig);
        String mon = getMon(key);
        log.info("datasource cache monitor: pool={}, {}", poolName(key), mon);
    }

    public static String getMon(String key) {
        DataSource ifPresent = dataSourceCache.getIfPresent(key);
        if (ifPresent instanceof HikariDataSource) {
            HikariPoolMXBean hikariPoolMXBean = ((HikariDataSource) ifPresent).getHikariPoolMXBean();
            if (hikariPoolMXBean == null) {
                return null;
            }
            int totalConnections = hikariPoolMXBean.getTotalConnections();
            int activeConnections = hikariPoolMXBean.getActiveConnections();
            int idleConnections = hikariPoolMXBean.getIdleConnections();
            int threadsAwaitingConnection = hikariPoolMXBean.getThreadsAwaitingConnection();
            return String.format("total: %d,active: %d, idle: %d, thread await: %d",
                    totalConnections, activeConnections, idleConnections, threadsAwaitingConnection);
        }
        return null;
    }

    private static String poolName(String key) {
        return "studio-rdbms-" + sha256(key).substring(0, 16);
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(bytes.length * 2);
            for (byte current : bytes) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is required for datasource pool identity", exception);
        }
    }

}
