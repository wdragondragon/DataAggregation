package com.jdragon.aggregation.core.sortmerge;

import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager;
import com.jdragon.aggregation.core.streaming.SourceRowScanner;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AdaptiveMergeCoordinatorTest {

    @Test
    public void resolvesOrderedSourcesWithoutOverflow() throws Exception {
        FakeSourceRowScanner scanner = new FakeSourceRowScanner();
        scanner.putRows("s1", rows(1, 2), 0L);
        scanner.putRows("s2", rows(1, 2), 0L);

        AdaptiveMergeConfig config = new AdaptiveMergeConfig();
        config.setPendingKeyThreshold(16);
        config.setPendingMemoryMB(64);
        config.setOverflowPartitionCount(4);

        OrderedKeySchema schema = new OrderedKeySchema(Arrays.asList("id"), Collections.<String, OrderedKeyType>emptyMap());
        AdaptiveMergeCoordinator coordinator = new AdaptiveMergeCoordinator(config, schema, Arrays.asList("s1", "s2"));
        List<String> resolvedKeys = new ArrayList<String>();

        AdaptiveMergeCoordinator.Result result = coordinator.execute(Arrays.asList(
                new OrderedSourceCursor(scanner, source("s1"), Arrays.asList("id"), false),
                new OrderedSourceCursor(scanner, source("s2"), Arrays.asList("id"), false)
        ), new AdaptiveMergeCoordinator.ResolvedGroupHandler() {
            @Override
            public void handle(OrderedKey key, Map<String, Map<String, Object>> firstRowsBySource) {
                resolvedKeys.add(key.getEncoded());
            }
        });

        try {
            assertEquals(Arrays.asList("1", "2"), resolvedKeys);
            assertEquals("sortmerge", result.getStats().getExecutionEngine());
            assertEquals(2L, result.getStats().getMergeResolvedKeyCount());
            assertTrue(!result.hasOverflow());
        } finally {
            if (result.getOverflowBucketStore() != null) {
                result.getOverflowBucketStore().cleanup();
            }
        }
    }

    @Test
    public void spillsOldestPendingPrefixWhenThresholdExceeded() throws Exception {
        FakeSourceRowScanner scanner = new FakeSourceRowScanner();
        scanner.putRows("s1", rows(1, 2), 0L);
        scanner.putRows("s2", rows(2), 80L);

        AdaptiveMergeConfig config = new AdaptiveMergeConfig();
        config.setPendingKeyThreshold(1);
        config.setPendingMemoryMB(64);
        config.setOverflowPartitionCount(4);
        config.setOnMemoryExceeded(AdaptiveMergeConfig.MemoryExceededAction.SPILL_OLDEST);

        OrderedKeySchema schema = new OrderedKeySchema(Arrays.asList("id"), Collections.<String, OrderedKeyType>emptyMap());
        AdaptiveMergeCoordinator coordinator = new AdaptiveMergeCoordinator(config, schema, Arrays.asList("s1", "s2"));
        List<String> resolvedKeys = new ArrayList<String>();

        AdaptiveMergeCoordinator.Result result = coordinator.execute(Arrays.asList(
                new OrderedSourceCursor(scanner, source("s1"), Arrays.asList("id"), false),
                new OrderedSourceCursor(scanner, source("s2"), Arrays.asList("id"), false)
        ), new AdaptiveMergeCoordinator.ResolvedGroupHandler() {
            @Override
            public void handle(OrderedKey key, Map<String, Map<String, Object>> firstRowsBySource) {
                resolvedKeys.add(key.getEncoded());
            }
        });

        try {
            assertEquals(Collections.singletonList("2"), resolvedKeys);
            assertEquals("hybrid", result.getStats().getExecutionEngine());
            assertEquals(1L, result.getStats().getMergeResolvedKeyCount());
            assertEquals(1L, result.getStats().getMergeSpilledKeyCount());
            assertNotNull(result.getOverflowBucketStore());
            assertTrue(result.getOverflowBucketStore().getSpilledRows() >= 1L);
        } finally {
            if (result.getOverflowBucketStore() != null) {
                result.getOverflowBucketStore().cleanup();
            }
        }
    }

    @Test
    public void switchesToBucketModeOnOrderViolation() throws Exception {
        FakeSourceRowScanner scanner = new FakeSourceRowScanner();
        scanner.putRows("s1", rows(2, 1), 0L);
        scanner.putRows("s2", rows(2), 80L);

        AdaptiveMergeConfig config = new AdaptiveMergeConfig();
        config.setPendingKeyThreshold(8);
        config.setPendingMemoryMB(64);
        config.setOverflowPartitionCount(4);
        config.setOnOrderViolation(AdaptiveMergeConfig.OrderViolationAction.BUCKET);

        OrderedKeySchema schema = new OrderedKeySchema(Arrays.asList("id"), Collections.<String, OrderedKeyType>emptyMap());
        AdaptiveMergeCoordinator coordinator = new AdaptiveMergeCoordinator(config, schema, Arrays.asList("s1", "s2"));

        AdaptiveMergeCoordinator.Result result = coordinator.execute(Arrays.asList(
                new OrderedSourceCursor(scanner, source("s1"), Arrays.asList("id"), false),
                new OrderedSourceCursor(scanner, source("s2"), Arrays.asList("id"), false)
        ), new AdaptiveMergeCoordinator.ResolvedGroupHandler() {
            @Override
            public void handle(OrderedKey key, Map<String, Map<String, Object>> firstRowsBySource) {
                // no-op
            }
        });

        try {
            assertEquals("bucket", result.getStats().getExecutionEngine());
            assertTrue(result.getStats().getFallbackReason().contains("out-of-order"));
            assertTrue(result.hasOverflow());
        } finally {
            if (result.getOverflowBucketStore() != null) {
                result.getOverflowBucketStore().cleanup();
            }
        }
    }

    private static DataSourceConfig source(String sourceId) {
        DataSourceConfig config = new DataSourceConfig();
        config.setSourceId(sourceId);
        config.setSourceName(sourceId);
        config.setPluginName("localfile");
        return config;
    }

    private static List<Map<String, Object>> rows(int... ids) {
        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        for (int id : ids) {
            Map<String, Object> row = new LinkedHashMap<String, Object>();
            row.put("id", id);
            rows.add(row);
        }
        return rows;
    }

    private static class FakeSourceRowScanner extends SourceRowScanner {
        private final Map<String, List<Map<String, Object>>> rowsBySource = new ConcurrentHashMap<String, List<Map<String, Object>>>();
        private final Map<String, Long> delayBySource = new ConcurrentHashMap<String, Long>();

        FakeSourceRowScanner() {
            super(new DataSourcePluginManager());
        }

        void putRows(String sourceId, List<Map<String, Object>> rows, long delayMs) {
            rowsBySource.put(sourceId, rows);
            delayBySource.put(sourceId, delayMs);
        }

        @Override
        public void scan(DataSourceConfig config, java.util.function.Consumer<Map<String, Object>> consumer) {
            List<Map<String, Object>> rows = rowsBySource.get(config.getSourceId());
            if (rows == null) {
                return;
            }
            Long delay = delayBySource.get(config.getSourceId());
            for (Map<String, Object> row : rows) {
                if (delay != null && delay > 0) {
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                }
                consumer.accept(new LinkedHashMap<String, Object>(row));
            }
        }
    }
}
