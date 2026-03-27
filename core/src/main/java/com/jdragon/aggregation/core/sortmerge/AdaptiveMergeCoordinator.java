package com.jdragon.aggregation.core.sortmerge;

import com.jdragon.aggregation.core.streaming.RowCodec;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * sort-merge 实验链路的共享调度器。
 *
 * <p>它负责消费多个有序来源产出的 key group，只把暂时无法判定的 key 留在
 * {@link PendingWindow} 中；一旦所有来源都已经读到该 key，或明确越过该 key，
 * 就立即触发业务侧处理。若等待窗口超出阈值，则把未决 key 写入
 * {@link OverflowBucketStore}，让执行流程退化为 hybrid/bucket 模式继续完成。
 */
public class AdaptiveMergeCoordinator {

    /**
     * 某个 key 在内存窗口内已经可以确定最终参与方时，回调业务处理逻辑。
     */
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
    private final OrderedKeySchema schema;
    private final List<String> sourceOrder;
    private final PendingWindow pendingWindow;
    private final SortMergeStats stats = new SortMergeStats();
    private final Map<String, OrderedKey> lastReadKeyBySource = new LinkedHashMap<String, OrderedKey>();
    private final Map<String, OrderedSourceCursor> cursorIndex = new LinkedHashMap<String, OrderedSourceCursor>();

    private OverflowBucketStore overflowBucketStore;
    /**
     * 已经转入桶处理的 key 上界。后续如果又读到不大于该上界的 key，也必须直接进桶，
     * 否则同一段 key 可能一部分走内存归并、一部分走桶回放，导致语义不一致。
     */
    private OrderedKey bucketUpperBound;
    /**
     * 一旦置为 true，后续事件不再尝试进入 pendingWindow，而是直接写入 overflow bucket。
     */
    private boolean bucketMode;

    public AdaptiveMergeCoordinator(AdaptiveMergeConfig config,
                                    OrderedKeySchema schema,
                                    List<String> sourceOrder) {
        this.config = config;
        this.schema = schema;
        this.sourceOrder = sourceOrder;
        this.pendingWindow = new PendingWindow(schema);
    }

    public Result execute(List<OrderedSourceCursor> cursors, ResolvedGroupHandler handler) throws Exception {
        for (OrderedSourceCursor cursor : cursors) {
            cursorIndex.put(cursor.getSourceId(), cursor);
            // 每个 source 启动一个生产线程，持续把 group 投递到自己的事件队列中。
            cursor.start();
        }

        int cursorIndexHint = 0;
        while (true) {
            boolean progressed = resolveReadyKeys(handler);

            if (allCursorsFinished() && pendingWindow.isEmpty()) {
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
                    if (!resolveReadyKeys(handler)) {
                        flushAllPendingToBucket();
                        break;
                    }
                    continue;
                }
                event = cursor.takeEvent();
                cursorIndexHint = (cursors.indexOf(cursor) + 1) % Math.max(1, cursors.size());
            }

            progressed = handleEvent(event, handler) || progressed;
            if (!progressed && allCursorsFinished()) {
                flushAllPendingToBucket();
            }
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

    private boolean handleEvent(OrderedSourceCursor.CursorEvent event, ResolvedGroupHandler handler) throws Exception {
        if (event == null) {
            return false;
        }
        if (event.getError() != null) {
            throw new RuntimeException("Failed to scan ordered source: " + event.getSourceId(), event.getError());
        }
        if (event.isEndOfStream()) {
            OrderedSourceCursor cursor = cursorIndex.get(event.getSourceId());
            if (cursor != null) {
                // 这里把 cursor 标记为 finished 后，当前调度线程在 isResolvable 中就会把
                // “该来源不会再产出更小 key” 视为已知事实，从而释放部分 pending key。
                cursor.setFinished(true);
            }
            return resolveReadyKeys(handler);
        }

        OrderedKeyGroup group = event.getGroup();
        stats.setDuplicateIgnoredCount(stats.getDuplicateIgnoredCount() + group.getDuplicateCount());
        OrderedKey previous = lastReadKeyBySource.get(group.getSourceId());
        if (previous != null && schema.compare(group.getKey(), previous) < 0) {
            String reason = "Detected out-of-order key for source " + group.getSourceId();
            if (config.getOnOrderViolation() == AdaptiveMergeConfig.OrderViolationAction.FAIL) {
                throw new IllegalStateException(reason);
            }
            switchToBucketMode(reason);
        }
        lastReadKeyBySource.put(group.getSourceId(), group.getKey());

        if (bucketMode || shouldRouteDirectlyToBucket(group.getKey())) {
            appendGroupToBucket(group);
            return true;
        }

        pendingWindow.add(group, estimateGroupBytes(group));
        if (pendingWindow.size() > config.getPendingKeyThreshold()
                || pendingWindow.getEstimatedBytes() > config.getPendingMemoryBytes()) {
            if (config.getOnMemoryExceeded() == AdaptiveMergeConfig.MemoryExceededAction.BUCKET) {
                switchToBucketMode("Pending window exceeded threshold");
            } else {
                spillOldestPending();
                stats.setExecutionEngine("hybrid");
            }
        }
        return resolveReadyKeys(handler);
    }

    private boolean resolveReadyKeys(ResolvedGroupHandler handler) throws Exception {
        boolean resolved = false;
        while (!pendingWindow.isEmpty()) {
            PendingWindow.PendingEntry entry = pendingWindow.firstEntry();
            if (entry == null || !isResolvable(entry)) {
                break;
            }
            pendingWindow.remove(entry.getKey());
            handler.handle(entry.getKey(), copySourceRows(entry.getFirstRowsBySource()));
            stats.setMergeResolvedKeyCount(stats.getMergeResolvedKeyCount() + 1);
            resolved = true;
        }
        return resolved;
    }

    private boolean isResolvable(PendingWindow.PendingEntry entry) {
        for (String sourceId : sourceOrder) {
            if (entry.getFirstRowsBySource().containsKey(sourceId)) {
                continue;
            }
            OrderedSourceCursor cursor = cursorIndex.get(sourceId);
            if (cursor == null || cursor.isFinished()) {
                continue;
            }
            OrderedKey lastReadKey = lastReadKeyBySource.get(sourceId);
            if (lastReadKey != null && schema.compare(lastReadKey, entry.getKey()) > 0) {
                continue;
            }
            return false;
        }
        return true;
    }

    private void spillOldestPending() {
        List<PendingWindow.PendingEntry> removed = pendingWindow.removeOldestUntilBelow(
                Math.max(1L, config.getPendingMemoryBytes() / 2L),
                Math.max(1, config.getPendingKeyThreshold() / 2)
        );
        if (removed.isEmpty()) {
            return;
        }
        ensureOverflowStore();
        for (PendingWindow.PendingEntry entry : removed) {
            spillEntry(entry);
            stats.setMergeSpilledKeyCount(stats.getMergeSpilledKeyCount() + 1);
        }
    }

    private void switchToBucketMode(String reason) {
        ensureOverflowStore();
        stats.setExecutionEngine(stats.getMergeResolvedKeyCount() == 0 && stats.getMergeSpilledKeyCount() == 0
                ? "bucket"
                : "hybrid");
        if (stats.getFallbackReason() == null) {
            stats.setFallbackReason(reason);
        }
        // 一旦切到 bucketMode，后续主线程消费到的新 group 都会直接落桶；
        // 与此同时，各 source 的生产线程仍会继续往事件队列投递数据，直到读完。
        bucketMode = true;
        flushAllPendingToBucket();
    }

    private void flushAllPendingToBucket() {
        ensureOverflowStore();
        while (!pendingWindow.isEmpty()) {
            PendingWindow.PendingEntry entry = pendingWindow.firstEntry();
            if (entry == null) {
                break;
            }
            pendingWindow.remove(entry.getKey());
            spillEntry(entry);
            stats.setMergeSpilledKeyCount(stats.getMergeSpilledKeyCount() + 1);
        }
    }

    private void spillEntry(PendingWindow.PendingEntry entry) {
        for (Map.Entry<String, Map<String, Object>> sourceEntry : entry.getFirstRowsBySource().entrySet()) {
            overflowBucketStore.append(sourceEntry.getKey(), entry.getKey(), sourceEntry.getValue());
        }
        if (bucketUpperBound == null || schema.compare(entry.getKey(), bucketUpperBound) > 0) {
            bucketUpperBound = entry.getKey();
        }
    }

    private boolean shouldRouteDirectlyToBucket(OrderedKey key) {
        return bucketUpperBound != null && schema.compare(key, bucketUpperBound) <= 0;
    }

    private void appendGroupToBucket(OrderedKeyGroup group) {
        ensureOverflowStore();
        overflowBucketStore.append(group.getSourceId(), group.getKey(), group.getFirstRow());
        if (bucketUpperBound == null || schema.compare(group.getKey(), bucketUpperBound) > 0) {
            bucketUpperBound = group.getKey();
        }
    }

    private void ensureOverflowStore() {
        if (overflowBucketStore == null) {
            overflowBucketStore = new OverflowBucketStore(
                    "sortmerge-overflow",
                    config.getOverflowSpillPath(),
                    config.getOverflowPartitionCount(),
                    false
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
