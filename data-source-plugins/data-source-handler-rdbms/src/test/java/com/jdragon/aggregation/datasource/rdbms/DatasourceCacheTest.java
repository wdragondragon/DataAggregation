package com.jdragon.aggregation.datasource.rdbms;

import com.github.benmanes.caffeine.cache.Cache;
import org.junit.Test;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

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
}
