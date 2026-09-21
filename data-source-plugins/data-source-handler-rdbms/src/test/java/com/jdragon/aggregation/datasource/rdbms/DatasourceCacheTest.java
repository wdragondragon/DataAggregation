package com.jdragon.aggregation.datasource.rdbms;

import com.github.benmanes.caffeine.cache.Cache;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.DataSourcePoolConfig;
import com.jdragon.aggregation.datasource.DataSourceType;
import com.jdragon.aggregation.datasource.rdbms.DefaultSourceSql;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.Test;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.sql.Connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DatasourceCacheTest {

    @SuppressWarnings("unchecked")
    private Cache<String, DataSource> getCache() throws Exception {
        Field cacheField = DatasourceCache.class.getDeclaredField("dataSourceCache");
        cacheField.setAccessible(true);
        return (Cache<String, DataSource>) cacheField.get(null);
    }

    @Test
    public void testConcurrentGetReturnsSingleDataSourceInstance() throws Exception {
        String jdbcUrl = "jdbc:h2:mem:datasource-cache-race;MODE=MySQL;DB_CLOSE_DELAY=-1";
        String driver = "org.h2.Driver";
        String user = "sa";
        String password = "";
        String key = String.join("*", jdbcUrl, driver, user, password);

        Cache<String, DataSource> cache = getCache();
        cache.invalidate(key);

        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Callable<DataSource>> tasks = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                tasks.add(() -> DatasourceCache.get(jdbcUrl, driver, user, password, "SELECT 1"));
            }

            List<Future<DataSource>> futures = executor.invokeAll(tasks);
            Set<Integer> identities = new HashSet<>();
            for (Future<DataSource> future : futures) {
                identities.add(System.identityHashCode(future.get()));
            }

            assertEquals("Concurrent cache access should reuse a single HikariDataSource instance", 1, identities.size());
        } finally {
            executor.shutdownNow();
            cache.invalidate(key);
            cache.cleanUp();
        }
    }

    @Test
    public void testDefaultPoolConfigurationIsPreserved() throws Exception {
        DataSourcePoolConfig config = DataSourcePoolConfig.from(null);

        assertEquals(DataSourcePoolConfig.DEFAULT_MAXIMUM_POOL_SIZE, config.getMaximumPoolSize());
        assertEquals(DataSourcePoolConfig.DEFAULT_MINIMUM_IDLE, config.getMinimumIdle());
        assertEquals(DataSourcePoolConfig.DEFAULT_CONNECTION_TIMEOUT_MS, config.getConnectionTimeoutMs());
        assertEquals(DataSourcePoolConfig.DEFAULT_IDLE_TIMEOUT_MS, config.getIdleTimeoutMs());
        assertEquals(DataSourcePoolConfig.DEFAULT_MAX_LIFETIME_MS, config.getMaxLifetimeMs());
        assertEquals(DataSourcePoolConfig.DEFAULT_KEEPALIVE_TIME_MS, config.getKeepaliveTimeMs());
        assertTrue(config.isDefault());
    }

    @Test
    public void testDatasourceDtoDefaultsToHistoricalPooledBehavior() {
        BaseDataSourceDTO dataSource = new BaseDataSourceDTO();

        assertTrue("An omitted switch must keep the historical pooled mode",
                dataSource.isUsePool());

        dataSource.setUsePool(false);
        assertFalse("An explicit false must select direct JDBC mode",
                dataSource.isUsePool());
    }

    @Test
    public void testDatasourcePoolSwitchUsesTolerantDatasourceMetadataValues() {
        Map<String, Object> metadata = new HashMap<String, Object>();

        assertTrue(DataSourcePoolConfig.isEnabled(metadata));

        metadata.put("usePool", " OFF ");
        assertFalse(DataSourcePoolConfig.isEnabled(metadata));

        metadata.put("usePool", Integer.valueOf(1));
        assertTrue(DataSourcePoolConfig.isEnabled(metadata));

        metadata.put("usePool", "yes");
        assertTrue(DataSourcePoolConfig.isEnabled(metadata));

        metadata.put("usePool", "false");
        assertFalse(DataSourcePoolConfig.isEnabled(metadata));
    }

    @Test
    public void testNestedDatasourcePoolSwitchIsUsedWhenDirectFieldIsMissing() {
        Map<String, Object> nested = new HashMap<String, Object>();
        nested.put("usePool", "false");

        Map<String, Object> metadata = new HashMap<String, Object>();
        metadata.put("extraParams", nested);

        assertFalse(DataSourcePoolConfig.isEnabled(metadata));

        metadata.put("usePool", Boolean.TRUE);
        assertTrue(DataSourcePoolConfig.isEnabled(metadata));
    }

    @Test
    public void testNestedJsonDatasourcePoolSwitchIsUsedWhenDirectFieldIsMissing() {
        Map<String, Object> metadata = new HashMap<String, Object>();
        metadata.put("extraParams", "{\"usePool\":false}");

        assertFalse(DataSourcePoolConfig.isEnabled(metadata));
    }

    @Test
    public void testDirectPoolFieldsOverrideNestedLegacyParameters() {
        Map<String, Object> nested = new HashMap<String, Object>();
        nested.put(DataSourcePoolConfig.MAXIMUM_POOL_SIZE, "12");
        nested.put(DataSourcePoolConfig.MINIMUM_IDLE, "1");

        Map<String, Object> metadata = new HashMap<String, Object>();
        metadata.put("extraParams", nested);
        metadata.put(DataSourcePoolConfig.MAXIMUM_POOL_SIZE, 40);
        metadata.put(DataSourcePoolConfig.MINIMUM_IDLE, 4);

        DataSourcePoolConfig config = DataSourcePoolConfig.fromDatasourceMetadata(metadata);

        assertEquals(40, config.getMaximumPoolSize());
        assertEquals(4, config.getMinimumIdle());
    }

    @Test
    public void testJsonStringExtraParamsAreAppliedToDatasourcePoolConfiguration() {
        Map<String, Object> metadata = new HashMap<String, Object>();
        metadata.put("extraParams", "{\"poolMaximumSize\":9,\"poolMinimumIdle\":1,"
                + "\"poolConnectionTimeoutMs\":5000}");

        DataSourcePoolConfig config = DataSourcePoolConfig.fromDatasourceMetadata(metadata);

        assertEquals(9, config.getMaximumPoolSize());
        assertEquals(1, config.getMinimumIdle());
        assertEquals(5000L, config.getConnectionTimeoutMs());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMalformedJsonExtraParamsDoNotSilentlyDropPoolSettings() {
        Map<String, Object> metadata = new HashMap<String, Object>();
        metadata.put("extraParams", "{\"poolMaximumSize\":}");

        DataSourcePoolConfig.fromDatasourceMetadata(metadata);
    }

    @Test
    public void testCustomPoolConfigurationIsAppliedToHikari() throws Exception {
        String jdbcUrl = "jdbc:h2:mem:datasource-cache-custom;MODE=MySQL;DB_CLOSE_DELAY=-1";
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put(DataSourcePoolConfig.MAXIMUM_POOL_SIZE, 7);
        parameters.put(DataSourcePoolConfig.MINIMUM_IDLE, 1);
        parameters.put(DataSourcePoolConfig.CONNECTION_TIMEOUT_MS, 250L);
        parameters.put(DataSourcePoolConfig.IDLE_TIMEOUT_MS, 10000L);
        parameters.put(DataSourcePoolConfig.MAX_LIFETIME_MS, 60000L);
        parameters.put(DataSourcePoolConfig.KEEPALIVE_TIME_MS, 30000L);

        Cache<String, DataSource> cache = getCache();
        try {
            HikariDataSource dataSource = (HikariDataSource) DatasourceCache.get(
                    jdbcUrl, "org.h2.Driver", "sa", "", "SELECT 1", parameters);

            assertEquals(7, dataSource.getMaximumPoolSize());
            assertEquals(1, dataSource.getMinimumIdle());
            assertEquals(250L, dataSource.getConnectionTimeout());
            assertEquals(10000L, dataSource.getIdleTimeout());
            assertEquals(60000L, dataSource.getMaxLifetime());
            assertEquals(30000L, dataSource.getKeepaliveTime());
            assertTrue(dataSource.getPoolName().startsWith("studio-rdbms-"));
            assertFalse(dataSource.getPoolName().contains("jdbc"));
        } finally {
            cache.invalidateAll();
            cache.cleanUp();
        }
    }

    @Test
    public void testDirectPoolFieldsAreConvertedForStringPluginParameters() {
        Map<String, Object> metadata = new HashMap<String, Object>();
        metadata.put(DataSourcePoolConfig.MAXIMUM_POOL_SIZE, 7);
        metadata.put(DataSourcePoolConfig.MINIMUM_IDLE, 1L);

        Map<String, String> parameters = new HashMap<String, String>();
        DataSourcePoolConfig.copyDirectStringParameters(metadata, parameters);

        assertEquals("7", parameters.get(DataSourcePoolConfig.MAXIMUM_POOL_SIZE));
        assertEquals("1", parameters.get(DataSourcePoolConfig.MINIMUM_IDLE));
    }

    @Test
    public void testDifferentPoolConfigurationsDoNotShareDataSource() {
        String jdbcUrl = "jdbc:h2:mem:datasource-cache-partition;MODE=MySQL;DB_CLOSE_DELAY=-1";
        Map<String, Object> small = new HashMap<String, Object>();
        small.put(DataSourcePoolConfig.MAXIMUM_POOL_SIZE, 3);
        small.put(DataSourcePoolConfig.MINIMUM_IDLE, 1);
        Map<String, Object> large = new HashMap<String, Object>();
        large.put(DataSourcePoolConfig.MAXIMUM_POOL_SIZE, 6);
        large.put(DataSourcePoolConfig.MINIMUM_IDLE, 1);

        Cache<String, DataSource> cache;
        try {
            DataSource smallPool = DatasourceCache.get(jdbcUrl, "org.h2.Driver", "sa", "", "SELECT 1", small);
            DataSource largePool = DatasourceCache.get(jdbcUrl, "org.h2.Driver", "sa", "", "SELECT 1", large);
            assertNotSame(smallPool, largePool);
            assertEquals(3, ((HikariDataSource) smallPool).getMaximumPoolSize());
            assertEquals(6, ((HikariDataSource) largePool).getMaximumPoolSize());
        } finally {
            try {
                cache = getCache();
                cache.invalidateAll();
                cache.cleanUp();
            } catch (Exception ignored) {
                // The assertion above is the behavior under test; cleanup is best effort.
            }
        }
    }

    @Test
    public void testInvalidPoolConfigurationIsRejected() {
        Map<String, Object> invalid = new HashMap<String, Object>();
        invalid.put(DataSourcePoolConfig.MAXIMUM_POOL_SIZE, 0);
        assertIllegalArgument(invalid);

        invalid.clear();
        invalid.put(DataSourcePoolConfig.MINIMUM_IDLE, 21);
        assertIllegalArgument(invalid);

        invalid.clear();
        invalid.put(DataSourcePoolConfig.KEEPALIVE_TIME_MS, 60000L);
        invalid.put(DataSourcePoolConfig.MAX_LIFETIME_MS, 60000L);
        assertIllegalArgument(invalid);
    }

    @Test
    public void testUsePoolFalseOpensDirectJdbcConnectionWithoutCalciteOrHikariPool() throws Exception {
        String jdbcUrl = "jdbc:h2:mem:datasource-direct;MODE=MySQL;DB_CLOSE_DELAY=-1";
        BaseDataSourceDTO dataSource = new BaseDataSourceDTO();
        dataSource.setHost("ignored");
        dataSource.setPort("0");
        dataSource.setDatabase("direct");
        dataSource.setUserName("sa");
        dataSource.setPassword("");
        dataSource.setUsePool(false);

        Cache<String, DataSource> cache = getCache();
        cache.invalidateAll();
        try (Connection connection = new TestRdbmsSourcePlugin(jdbcUrl).getConnection(dataSource)) {
            assertTrue(connection.getClass().getName().startsWith("org.h2.jdbc."));
            assertFalse(connection.getClass().getName().contains("Hikari"));
            assertFalse(connection.getClass().getName().contains("BasicDataSource"));
            try (java.sql.ResultSet resultSet = connection.createStatement().executeQuery("SELECT 1")) {
                assertTrue(resultSet.next());
                assertEquals(1, resultSet.getInt(1));
            }
            assertTrue(cache.asMap().isEmpty());
        } finally {
            cache.invalidateAll();
            cache.cleanUp();
        }
    }

    @Test
    public void testDirectConnectionEvictsPreviouslyCachedPoolForSameDatasource() throws Exception {
        String jdbcUrl = "jdbc:h2:mem:datasource-direct-evict;MODE=MySQL;DB_CLOSE_DELAY=-1";
        Map<String, Object> parameters = new HashMap<String, Object>();
        parameters.put(DataSourcePoolConfig.MAXIMUM_POOL_SIZE, 3);
        parameters.put(DataSourcePoolConfig.MINIMUM_IDLE, 1);

        Cache<String, DataSource> cache = getCache();
        cache.invalidateAll();
        try {
            DatasourceCache.get(jdbcUrl, "org.h2.Driver", "sa", "", "SELECT 1", parameters);
            assertFalse(cache.asMap().isEmpty());

            BaseDataSourceDTO dataSource = new BaseDataSourceDTO();
            dataSource.setUserName("sa");
            dataSource.setPassword("");
            dataSource.setUsePool(false);

            try (Connection connection = new TestRdbmsSourcePlugin(jdbcUrl).getConnection(dataSource)) {
                assertTrue(connection.isValid(1));
                assertTrue(cache.asMap().isEmpty());
            }
        } finally {
            cache.invalidateAll();
            cache.cleanUp();
        }
    }

    private static final class TestRdbmsSourcePlugin extends RdbmsSourcePlugin {
        private final String jdbcUrl;

        private TestRdbmsSourcePlugin(String jdbcUrl) {
            super(new DefaultSourceSql());
            this.jdbcUrl = jdbcUrl;
        }

        @Override
        public DataSourceType getType() {
            return DataSourceType.Mysql8;
        }

        @Override
        public String getDriver() {
            return "org.h2.Driver";
        }

        @Override
        public String getQuotationMarks() {
            return "`";
        }

        @Override
        public String getJdbc() {
            return jdbcUrl;
        }

        @Override
        public String joinJdbcUrl(BaseDataSourceDTO dataSource) {
            return jdbcUrl;
        }

        @Override
        public String getTestQuery() {
            return "SELECT 1";
        }
    }

    private void assertIllegalArgument(Map<String, ?> parameters) {
        try {
            DataSourcePoolConfig.from(parameters);
            fail("Expected invalid datasource pool settings to be rejected");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }
}
