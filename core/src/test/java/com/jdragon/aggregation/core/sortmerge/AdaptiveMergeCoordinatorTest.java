package com.jdragon.aggregation.core.sortmerge;

import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
        assertTrue(resolvedKeys.contains(encodedKey(1L)));
        assertTrue(resolvedKeys.contains(encodedKey(2L)));
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

        assertEquals(Arrays.asList(encodedKey(2L)), resolvedKeys);
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

    @Test
    public void shouldRouteUnorderedSourcesThroughOverflowWithoutImmediateResolve() throws Exception {
        AdaptiveMergeConfig config = config(8, 64);
        AdaptiveMergeCoordinator coordinator = new AdaptiveMergeCoordinator(
                config,
                Arrays.asList("sourceA", "sourceB")
        );

        List<String> resolvedKeys = new ArrayList<String>();
        AdaptiveMergeCoordinator.Result result = coordinator.execute(Arrays.<OrderedSourceCursor>asList(
                unorderedCursor("sourceA",
                        group("sourceA", 1L, "a-first"),
                        group("sourceA", 2L, "a-2"),
                        group("sourceA", 1L, "a-late"),
                        end("sourceA")),
                unorderedCursor("sourceB",
                        group("sourceB", 1L, "b-1"),
                        end("sourceB"))
        ), (key, rowsBySource) -> resolvedKeys.add(key.getEncoded()));

        assertTrue(resolvedKeys.isEmpty());
        assertEquals("hybrid", result.getStats().getExecutionEngine());
        assertEquals("Unordered source detected", result.getStats().getFallbackReason());
        Map<String, LinkedHashMap<String, Map<String, Object>>> overflowGroups = readOverflowGroups(result.getOverflowBucketStore());
        assertEquals("a-first", overflowGroups.get(encodedKey(1L)).get("sourceA").get("value"));
        assertEquals("b-1", overflowGroups.get(encodedKey(1L)).get("sourceB").get("value"));
        result.getOverflowBucketStore().cleanup();
    }

    @Test
    public void shouldUseOverflowWhenAdaptiveMergeIsDisabled() throws Exception {
        AdaptiveMergeConfig config = config(8, 64);
        config.setEnabled(false);
        AdaptiveMergeCoordinator coordinator = new AdaptiveMergeCoordinator(
                config,
                Arrays.asList("sourceA", "sourceB")
        );

        List<String> resolvedKeys = new ArrayList<String>();
        AdaptiveMergeCoordinator.Result result = coordinator.execute(Arrays.<OrderedSourceCursor>asList(
                cursor("sourceA", group("sourceA", 1L, "a-1"), end("sourceA")),
                cursor("sourceB", group("sourceB", 1L, "b-1"), end("sourceB"))
        ), (key, rowsBySource) -> resolvedKeys.add(key.getEncoded()));

        assertTrue(resolvedKeys.isEmpty());
        assertNotNull(result.getOverflowBucketStore());
        assertEquals("Adaptive merge disabled", result.getStats().getFallbackReason());
        result.getOverflowBucketStore().cleanup();
    }

    @Test
    public void shouldCancelUnfinishedCursorsAndCleanupOverflowWhenHandlerFails() throws Exception {
        AdaptiveMergeConfig config = config(8, 64);
        AdaptiveMergeCoordinator coordinator = new AdaptiveMergeCoordinator(
                config,
                Arrays.asList("sourceA", "sourceB")
        );
        StubCursor sourceA = cursor("sourceA", group("sourceA", 1L, "a-1"));
        StubCursor sourceB = cursor("sourceB", group("sourceB", 1L, "b-1"));

        try {
            coordinator.execute(Arrays.<OrderedSourceCursor>asList(sourceA, sourceB), (key, rowsBySource) -> {
                throw new IllegalStateException("boom");
            });
        } catch (IllegalStateException expected) {
            assertEquals("boom", expected.getMessage());
        }

        assertTrue(sourceA.cancelCalled);
        assertTrue(sourceB.cancelCalled);
    }

    @Test
    public void shouldCleanupOverflowStoreWhenCursorFailsAfterSpill() throws Exception {
        Path spillRoot = Files.createTempDirectory("adaptive-merge-failure-root");
        try {
            AdaptiveMergeConfig config = config(1, 64);
            config.setOverflowSpillPath(spillRoot.toAbsolutePath().toString());
            AdaptiveMergeCoordinator coordinator = new AdaptiveMergeCoordinator(
                    config,
                    Arrays.asList("sourceA", "sourceB")
            );
            StubCursor sourceA = cursor("sourceA",
                    group("sourceA", 1L, "a-1"),
                    group("sourceA", 2L, "a-2"),
                    end("sourceA"));
            StubCursor sourceB = delayedCursor("sourceB", 1,
                    error("sourceB", new IllegalStateException("scan failed")));

            try {
                coordinator.execute(Arrays.<OrderedSourceCursor>asList(sourceA, sourceB),
                        (key, rowsBySource) -> {
                        });
                fail("Expected cursor failure to abort coordinator execution");
            } catch (RuntimeException expected) {
                assertTrue(expected.getMessage().contains("Failed to scan source"));
            }

            assertEquals(0L, countChildren(spillRoot));
            assertTrue(sourceA.cancelCalled);
            assertTrue(sourceB.cancelCalled);
        } finally {
            cleanupDirectory(spillRoot);
        }
    }

    @Test
    public void shouldRespectMaxActiveCursorLimit() throws Exception {
        AdaptiveMergeConfig config = config(8, 64);
        config.setMaxActiveCursors(1);
        AdaptiveMergeCoordinator coordinator = new AdaptiveMergeCoordinator(
                config,
                Arrays.asList("sourceA", "sourceB", "sourceC")
        );
        AtomicInteger activeCursors = new AtomicInteger();
        AtomicInteger maxObservedActiveCursors = new AtomicInteger();

        StubCursor sourceA = cursorWithCounters("sourceA", activeCursors, maxObservedActiveCursors,
                group("sourceA", 1L, "a-1"), end("sourceA"));
        StubCursor sourceB = cursorWithCounters("sourceB", activeCursors, maxObservedActiveCursors,
                group("sourceB", 1L, "b-1"), end("sourceB"));
        StubCursor sourceC = cursorWithCounters("sourceC", activeCursors, maxObservedActiveCursors,
                group("sourceC", 1L, "c-1"), end("sourceC"));

        List<String> resolvedKeys = new ArrayList<String>();
        coordinator.execute(Arrays.<OrderedSourceCursor>asList(sourceA, sourceB, sourceC),
                (key, rowsBySource) -> resolvedKeys.add(key.getEncoded()));

        assertEquals(1, maxObservedActiveCursors.get());
        assertEquals(Arrays.asList(encodedKey(1L)), resolvedKeys);
        assertEquals(1, sourceA.startCount);
        assertEquals(1, sourceB.startCount);
        assertEquals(1, sourceC.startCount);
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

    private static OrderedSourceCursor.CursorEvent error(String sourceId, Throwable error) {
        return OrderedSourceCursor.CursorEvent.error(sourceId, error);
    }

    private static Map<String, Object> row(long bizId, String value) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("biz_id", bizId);
        row.put("value", value);
        return row;
    }

    private static StubCursor cursor(String sourceId, OrderedSourceCursor.CursorEvent... events) {
        return new StubCursor(sourceId, 0, true, null, null, events);
    }

    private static StubCursor unorderedCursor(String sourceId, OrderedSourceCursor.CursorEvent... events) {
        return new StubCursor(sourceId, 0, false, null, null, events);
    }

    private static StubCursor delayedCursor(String sourceId,
                                            int nullPollsBeforeFirstEvent,
                                            OrderedSourceCursor.CursorEvent... events) {
        return new StubCursor(sourceId, nullPollsBeforeFirstEvent, true, null, null, events);
    }

    private static StubCursor cursorWithCounters(String sourceId,
                                                 AtomicInteger activeCursors,
                                                 AtomicInteger maxObservedActiveCursors,
                                                 OrderedSourceCursor.CursorEvent... events) {
        return new StubCursor(sourceId, 0, true, activeCursors, maxObservedActiveCursors, events);
    }

    private static String encodedKey(long bizId) {
        return OrderedKey.fromRow(row(bizId, "key"), Arrays.asList("biz_id")).getEncoded();
    }

    private static Map<String, LinkedHashMap<String, Map<String, Object>>> readOverflowGroups(OverflowBucketStore store) throws Exception {
        Map<String, LinkedHashMap<String, Map<String, Object>>> groups =
                new LinkedHashMap<String, LinkedHashMap<String, Map<String, Object>>>();
        for (int partition = 0; partition < store.getStore().getPartitionCount(); partition++) {
            if (!store.getStore().partitionExists(partition)) {
                continue;
            }
            try (com.jdragon.aggregation.core.streaming.PartitionReader reader =
                         store.getStore().openPartitionReader(partition)) {
                reader.readAll(row -> {
                    LinkedHashMap<String, Map<String, Object>> sourceRows = groups.get(row.getKey());
                    if (sourceRows == null) {
                        sourceRows = new LinkedHashMap<String, Map<String, Object>>();
                        groups.put(row.getKey(), sourceRows);
                    }
                    sourceRows.putIfAbsent(row.getSourceId(), row.getRow());
                });
            }
        }
        return groups;
    }

    private static long countChildren(Path directory) throws Exception {
        try (Stream<Path> children = Files.list(directory)) {
            return children.count();
        }
    }

    private static void cleanupDirectory(Path directory) throws Exception {
        if (directory == null || !Files.exists(directory)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(directory)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (Exception ignored) {
                }
            });
        }
    }

    private static class StubCursor extends OrderedSourceCursor {
        private final Deque<CursorEvent> events = new ArrayDeque<CursorEvent>();
        private final boolean ordered;
        private final AtomicInteger activeCursors;
        private final AtomicInteger maxObservedActiveCursors;
        private int remainingNullPolls;
        private boolean active;
        private boolean cancelCalled;
        private int startCount;

        StubCursor(String sourceId,
                   int nullPollsBeforeFirstEvent,
                   boolean ordered,
                   AtomicInteger activeCursors,
                   AtomicInteger maxObservedActiveCursors,
                   CursorEvent... events) {
            super(null, dataSource(sourceId), Arrays.asList("biz_id"), false);
            this.ordered = ordered;
            this.activeCursors = activeCursors;
            this.maxObservedActiveCursors = maxObservedActiveCursors;
            this.remainingNullPolls = Math.max(0, nullPollsBeforeFirstEvent);
            this.events.addAll(Arrays.asList(events));
        }

        @Override
        public void start() {
            startCount++;
            if (activeCursors != null) {
                active = true;
                int current = activeCursors.incrementAndGet();
                while (true) {
                    int previous = maxObservedActiveCursors.get();
                    if (current <= previous || maxObservedActiveCursors.compareAndSet(previous, current)) {
                        break;
                    }
                }
            }
        }

        @Override
        public void setFinished(boolean finished) {
            super.setFinished(finished);
            if (finished && activeCursors != null && active) {
                active = false;
                activeCursors.decrementAndGet();
            }
        }

        @Override
        public void cancel() {
            cancelCalled = true;
            setFinished(true);
        }

        @Override
        public boolean isOrdered() {
            return ordered;
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
