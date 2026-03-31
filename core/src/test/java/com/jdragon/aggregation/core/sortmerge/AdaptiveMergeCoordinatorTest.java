package com.jdragon.aggregation.core.sortmerge;

import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AdaptiveMergeCoordinatorTest {

    @Test
    public void shouldResolveCompleteKeysWithoutRelyingOnMinimumKeyOrder() throws Exception {
        AdaptiveMergeConfig config = config(8, 64);
        AdaptiveMergeCoordinator coordinator = new AdaptiveMergeCoordinator(
                config,
                Arrays.asList("sourceA", "sourceB")
        );

        List<String> resolvedKeys = new ArrayList<String>();
        AdaptiveMergeCoordinator.Result result = coordinator.execute(Arrays.<OrderedSourceCursor>asList(
                cursor("sourceA",
                        group("sourceA", 2L, "a-2"),
                        group("sourceA", 1L, "a-1"),
                        end("sourceA")),
                cursor("sourceB",
                        group("sourceB", 1L, "b-1"),
                        group("sourceB", 2L, "b-2"),
                        end("sourceB"))
        ), (key, rowsBySource) -> {
            resolvedKeys.add(key.getEncoded());
            assertEquals(2, rowsBySource.size());
        });

        assertEquals(2, resolvedKeys.size());
        assertTrue(resolvedKeys.contains("1"));
        assertTrue(resolvedKeys.contains("2"));
        assertEquals("sortmerge", result.getStats().getExecutionEngine());
        assertEquals(2L, result.getStats().getMergeResolvedKeyCount());
        assertEquals(2L, result.getStats().getWindowImmediateResolvedKeyCount());
        assertEquals(0L, result.getStats().getMergeSpilledKeyCount());
    }

    @Test
    public void shouldSpillOldestPendingKeyAndRouteLateArrivalsDirectlyToOverflow() throws Exception {
        AdaptiveMergeConfig config = config(1, 64);
        AdaptiveMergeCoordinator coordinator = new AdaptiveMergeCoordinator(
                config,
                Arrays.asList("sourceA", "sourceB")
        );

        List<String> resolvedKeys = new ArrayList<String>();
        AdaptiveMergeCoordinator.Result result = coordinator.execute(Arrays.<OrderedSourceCursor>asList(
                cursor("sourceA",
                        group("sourceA", 1L, "a-1"),
                        group("sourceA", 2L, "a-2"),
                        end("sourceA")),
                delayedCursor("sourceB", 1,
                        group("sourceB", 1L, "b-1"),
                        group("sourceB", 2L, "b-2"),
                        end("sourceB"))
        ), (key, rowsBySource) -> resolvedKeys.add(key.getEncoded()));

        assertEquals(Arrays.asList("2"), resolvedKeys);
        assertNotNull(result.getOverflowBucketStore());
        assertEquals("hybrid", result.getStats().getExecutionEngine());
        assertEquals(1L, result.getStats().getMergeSpilledKeyCount());
        assertEquals(1L, result.getStats().getWindowEvictedKeyCount());
        assertEquals(1L, result.getStats().getSpillLateArrivalKeyCount());
        assertTrue(result.getOverflowBucketStore().getSpilledRows() >= 2L);
    }

    @Test
    public void shouldDrainIncompletePendingKeysWhenSourcesFinish() throws Exception {
        AdaptiveMergeConfig config = config(8, 64);
        AdaptiveMergeCoordinator coordinator = new AdaptiveMergeCoordinator(
                config,
                Arrays.asList("sourceA", "sourceB")
        );

        List<Map<String, Map<String, Object>>> resolvedGroups = new ArrayList<Map<String, Map<String, Object>>>();
        AdaptiveMergeCoordinator.Result result = coordinator.execute(Arrays.<OrderedSourceCursor>asList(
                cursor("sourceA",
                        group("sourceA", 1L, "a-1"),
                        end("sourceA")),
                cursor("sourceB", end("sourceB"))
        ), (key, rowsBySource) -> resolvedGroups.add(rowsBySource));

        assertEquals(1, resolvedGroups.size());
        assertEquals(1, resolvedGroups.get(0).size());
        assertTrue(resolvedGroups.get(0).containsKey("sourceA"));
        assertEquals(1L, result.getStats().getMergeResolvedKeyCount());
        assertEquals(0L, result.getStats().getWindowImmediateResolvedKeyCount());
        assertEquals(0L, result.getStats().getMergeSpilledKeyCount());
    }

    private static AdaptiveMergeConfig config(int pendingKeyThreshold, int pendingMemoryMB) throws Exception {
        AdaptiveMergeConfig config = new AdaptiveMergeConfig();
        config.setPendingKeyThreshold(pendingKeyThreshold);
        config.setPendingMemoryMB(pendingMemoryMB);
        config.setOverflowPartitionCount(4);
        Path spillDir = Files.createTempDirectory("adaptive-merge-test");
        config.setOverflowSpillPath(spillDir.toAbsolutePath().toString());
        return config;
    }

    private static OrderedSourceCursor.CursorEvent group(String sourceId, long bizId, String value) {
        OrderedKeyGroup group = new OrderedKeyGroup();
        group.setSourceId(sourceId);
        group.setKey(OrderedKey.fromRow(row(bizId, value), Arrays.asList("biz_id")));
        group.setFirstRow(row(bizId, value));
        group.setDuplicateCount(0);
        group.setScannedRecords(1L);
        return OrderedSourceCursor.CursorEvent.group(sourceId, group);
    }

    private static OrderedSourceCursor.CursorEvent end(String sourceId) {
        return OrderedSourceCursor.CursorEvent.end(sourceId);
    }

    private static Map<String, Object> row(long bizId, String value) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("biz_id", bizId);
        row.put("value", value);
        return row;
    }

    private static OrderedSourceCursor cursor(String sourceId, OrderedSourceCursor.CursorEvent... events) {
        return new StubCursor(sourceId, 0, events);
    }

    private static OrderedSourceCursor delayedCursor(String sourceId,
                                                     int nullPollsBeforeFirstEvent,
                                                     OrderedSourceCursor.CursorEvent... events) {
        return new StubCursor(sourceId, nullPollsBeforeFirstEvent, events);
    }

    private static class StubCursor extends OrderedSourceCursor {
        private final Deque<CursorEvent> events = new ArrayDeque<CursorEvent>();
        private int remainingNullPolls;

        StubCursor(String sourceId, int nullPollsBeforeFirstEvent, CursorEvent... events) {
            super(null, dataSource(sourceId), Arrays.asList("biz_id"), false);
            this.remainingNullPolls = Math.max(0, nullPollsBeforeFirstEvent);
            this.events.addAll(Arrays.asList(events));
        }

        @Override
        public void start() {
            // Tests feed events directly; no background scanner thread is needed.
        }

        @Override
        public CursorEvent pollEvent() {
            if (remainingNullPolls > 0) {
                remainingNullPolls--;
                return null;
            }
            return events.pollFirst();
        }

        @Override
        public CursorEvent takeEvent() {
            return events.pollFirst();
        }
    }

    private static DataSourceConfig dataSource(String sourceId) {
        DataSourceConfig config = new DataSourceConfig();
        config.setSourceId(sourceId);
        return config;
    }
}
