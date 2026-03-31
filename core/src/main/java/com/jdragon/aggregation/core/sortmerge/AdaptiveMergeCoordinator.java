package com.jdragon.aggregation.core.sortmerge;

import com.jdragon.aggregation.core.streaming.RowCodec;
import com.jdragon.aggregation.core.streaming.SpillGuard;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared coordinator for adaptive merge execution.
 *
 * <p>It keeps one pending entry per key, merges source rows as they arrive, and
 * emits groups immediately once all sources are present. Unfinished keys are
 * bounded by the pending window and spill to overflow in first-seen order.
 */
public class AdaptiveMergeCoordinator {

    public interface ResolvedGroupHandler {
        void handle(OrderedKey key, Map<String, Map<String, Object>> firstRowsBySource) throws Exception;
    }

    public static class Result {
        private final SortMergeStats stats;
        private final OverflowBucketStore overflowBucketStore;

        public Result(SortMergeStats stats, OverflowBucketStore overflowBucketStore) {
            this.stats = stats;
            this.overflowBucketStore = overflowBucketStore;
        }

        public SortMergeStats getStats() {
            return stats;
        }

        public OverflowBucketStore getOverflowBucketStore() {
            return overflowBucketStore;
        }

        public boolean hasOverflow() {
            return overflowBucketStore != null && overflowBucketStore.getSpilledRows() > 0;
        }
    }

    private final AdaptiveMergeConfig config;
    private final List<String> sourceOrder;
    private final PendingWindow pendingWindow = new PendingWindow();
    private final SpillGuard spillGuard;
    private final boolean keepTempFiles;
    private final SortMergeStats stats = new SortMergeStats();
    private final Map<String, OrderedSourceCursor> cursorIndex = new LinkedHashMap<String, OrderedSourceCursor>();
    private final Set<String> spilledEncodedKeys = new LinkedHashSet<String>();
    private final Set<String> lateArrivalEncodedKeys = new LinkedHashSet<String>();

    private OverflowBucketStore overflowBucketStore;

    public AdaptiveMergeCoordinator(AdaptiveMergeConfig config,
                                    List<String> sourceOrder) {
        this(config, sourceOrder, null, false);
    }

    public AdaptiveMergeCoordinator(AdaptiveMergeConfig config,
                                    List<String> sourceOrder,
                                    SpillGuard spillGuard) {
        this(config, sourceOrder, spillGuard, false);
    }

    public AdaptiveMergeCoordinator(AdaptiveMergeConfig config,
                                    List<String> sourceOrder,
                                    SpillGuard spillGuard,
                                    boolean keepTempFiles) {
        this.config = config;
        this.sourceOrder = sourceOrder;
        this.spillGuard = spillGuard;
        this.keepTempFiles = keepTempFiles;
    }

    public Result execute(List<OrderedSourceCursor> cursors, ResolvedGroupHandler handler) throws Exception {
        for (OrderedSourceCursor cursor : cursors) {
            cursorIndex.put(cursor.getSourceId(), cursor);
            cursor.start();
        }

        int cursorIndexHint = 0;
        while (true) {
            if (allCursorsFinished()) {
                drainRemainingPending(handler);
                break;
            }

            OrderedSourceCursor.CursorEvent event = null;
            for (int i = 0; i < cursors.size(); i++) {
                OrderedSourceCursor cursor = cursors.get((cursorIndexHint + i) % cursors.size());
                OrderedSourceCursor.CursorEvent candidate = cursor.pollEvent();
                if (candidate != null) {
                    event = candidate;
                    cursorIndexHint = (cursorIndexHint + i + 1) % Math.max(1, cursors.size());
                    break;
                }
            }

            if (event == null) {
                OrderedSourceCursor cursor = nextActiveCursor(cursors, cursorIndexHint);
                if (cursor == null) {
                    continue;
                }
                event = cursor.takeEvent();
                cursorIndexHint = (cursors.indexOf(cursor) + 1) % Math.max(1, cursors.size());
            }

            handleEvent(event, handler);
        }

        for (OrderedSourceCursor cursor : cursors) {
            stats.getSourceRecordCounts().put(cursor.getSourceId(), cursor.getScannedRecords());
            stats.getSourceGroupCounts().put(cursor.getSourceId(), cursor.getProducedGroups());
        }
        if (overflowBucketStore != null) {
            overflowBucketStore.close();
        }
        return new Result(stats, overflowBucketStore);
    }

    private void handleEvent(OrderedSourceCursor.CursorEvent event, ResolvedGroupHandler handler) throws Exception {
        if (event == null) {
            return;
        }
        if (event.getError() != null) {
            throw new RuntimeException("Failed to scan source: " + event.getSourceId(), event.getError());
        }
        if (event.isEndOfStream()) {
            OrderedSourceCursor cursor = cursorIndex.get(event.getSourceId());
            if (cursor != null) {
                cursor.setFinished(true);
            }
            return;
        }

        OrderedKeyGroup group = event.getGroup();
        stats.setDuplicateIgnoredCount(stats.getDuplicateIgnoredCount() + group.getDuplicateCount());

        String encodedKey = group.getKey().getEncoded();
        if (spilledEncodedKeys.contains(encodedKey)) {
            appendGroupToOverflow(group);
            if (lateArrivalEncodedKeys.add(encodedKey)) {
                stats.setSpillLateArrivalKeyCount(stats.getSpillLateArrivalKeyCount() + 1L);
            }
            return;
        }

        PendingWindow.PendingEntry entry = pendingWindow.add(group, estimateGroupBytes(group));
        stats.setPendingPeakKeyCount(Math.max(stats.getPendingPeakKeyCount(), pendingWindow.size()));

        if (isComplete(entry)) {
            PendingWindow.PendingEntry resolved = pendingWindow.remove(encodedKey);
            if (resolved != null) {
                handler.handle(resolved.getKey(), copySourceRows(resolved.getFirstRowsBySource()));
                stats.setMergeResolvedKeyCount(stats.getMergeResolvedKeyCount() + 1L);
                stats.setWindowImmediateResolvedKeyCount(stats.getWindowImmediateResolvedKeyCount() + 1L);
            }
            return;
        }

        if (pendingWindow.size() > config.getPendingKeyThreshold()
                || pendingWindow.getEstimatedBytes() > config.getPendingMemoryBytes()) {
            spillOldestPending();
        }
    }

    private boolean isComplete(PendingWindow.PendingEntry entry) {
        return entry != null && entry.getFirstRowsBySource().size() >= sourceOrder.size();
    }

    private void spillOldestPending() {
        List<PendingWindow.PendingEntry> removed = pendingWindow.removeOldestUntilBelow(
                config.getPendingMemoryBytes(),
                config.getPendingKeyThreshold()
        );
        if (removed.isEmpty()) {
            return;
        }
        ensureOverflowStore();
        stats.setExecutionEngine("hybrid");
        if (stats.getFallbackReason() == null) {
            stats.setFallbackReason("Pending window exceeded threshold");
        }
        for (PendingWindow.PendingEntry entry : removed) {
            spillEntry(entry);
        }
    }

    private void drainRemainingPending(ResolvedGroupHandler handler) throws Exception {
        List<PendingWindow.PendingEntry> remaining = pendingWindow.snapshotEntries();
        for (PendingWindow.PendingEntry entry : remaining) {
            PendingWindow.PendingEntry removed = pendingWindow.remove(entry.getEncodedKey());
            if (removed == null) {
                continue;
            }
            handler.handle(removed.getKey(), copySourceRows(removed.getFirstRowsBySource()));
            stats.setMergeResolvedKeyCount(stats.getMergeResolvedKeyCount() + 1L);
        }
    }

    private void spillEntry(PendingWindow.PendingEntry entry) {
        if (entry == null) {
            return;
        }
        ensureOverflowStore();
        if (spilledEncodedKeys.add(entry.getEncodedKey())) {
            stats.setMergeSpilledKeyCount(stats.getMergeSpilledKeyCount() + 1L);
            stats.setWindowEvictedKeyCount(stats.getWindowEvictedKeyCount() + 1L);
        }
        for (Map.Entry<String, Map<String, Object>> sourceEntry : entry.getFirstRowsBySource().entrySet()) {
            overflowBucketStore.append(sourceEntry.getKey(), entry.getKey(), sourceEntry.getValue());
        }
    }

    private void appendGroupToOverflow(OrderedKeyGroup group) {
        ensureOverflowStore();
        overflowBucketStore.append(group.getSourceId(), group.getKey(), group.getFirstRow());
    }

    private void ensureOverflowStore() {
        if (overflowBucketStore == null) {
            overflowBucketStore = new OverflowBucketStore(
                    "sortmerge-overflow",
                    config.getOverflowSpillPath(),
                    config.getOverflowPartitionCount(),
                    keepTempFiles,
                    spillGuard
            );
        }
    }

    private boolean allCursorsFinished() {
        for (OrderedSourceCursor cursor : cursorIndex.values()) {
            if (!cursor.isFinished()) {
                return false;
            }
        }
        return true;
    }

    private OrderedSourceCursor nextActiveCursor(List<OrderedSourceCursor> cursors, int startIndex) {
        for (int i = 0; i < cursors.size(); i++) {
            OrderedSourceCursor cursor = cursors.get((startIndex + i) % cursors.size());
            if (!cursor.isFinished()) {
                return cursor;
            }
        }
        return null;
    }

    private long estimateGroupBytes(OrderedKeyGroup group) {
        int rowBytes = RowCodec.encode(group.getFirstRow()).getBytes(StandardCharsets.UTF_8).length;
        int keyBytes = group.getKey().getEncoded().getBytes(StandardCharsets.UTF_8).length;
        return rowBytes + keyBytes + 128L;
    }

    private Map<String, Map<String, Object>> copySourceRows(Map<String, Map<String, Object>> sourceRows) {
        Map<String, Map<String, Object>> copied = new LinkedHashMap<String, Map<String, Object>>();
        for (Map.Entry<String, Map<String, Object>> entry : sourceRows.entrySet()) {
            copied.put(entry.getKey(), new LinkedHashMap<String, Object>(entry.getValue()));
        }
        return copied;
    }
}
