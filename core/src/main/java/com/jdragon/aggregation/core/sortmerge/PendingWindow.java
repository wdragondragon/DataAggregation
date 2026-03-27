package com.jdragon.aggregation.core.sortmerge;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * 尚未可判定 key 的有序内存等待区。
 *
 * <p>每个 entry 只保存“每个 source 的首条记录”和一个轻量级大小估算，不会持有完整
 * 的 source 流数据。某个 key 一旦可判定，或被溢写到桶中，对应内存即可立即释放。
 */
public class PendingWindow {

    /**
     * 当前等待窗口中的一个未决 key。
     */
    public static class PendingEntry {
        private final OrderedKey key;
        private final Map<String, Map<String, Object>> firstRowsBySource = new LinkedHashMap<String, Map<String, Object>>();
        private long estimatedBytes;

        PendingEntry(OrderedKey key) {
            this.key = key;
        }

        public OrderedKey getKey() {
            return key;
        }

        public Map<String, Map<String, Object>> getFirstRowsBySource() {
            return firstRowsBySource;
        }

        public long getEstimatedBytes() {
            return estimatedBytes;
        }

        void add(String sourceId, Map<String, Object> row, long bytes) {
            if (!firstRowsBySource.containsKey(sourceId)) {
                firstRowsBySource.put(sourceId, row);
                estimatedBytes += bytes;
            }
        }
    }

    private final NavigableMap<OrderedKey, PendingEntry> entries;
    private long estimatedBytes;

    public PendingWindow(final OrderedKeySchema schema) {
        this.entries = new TreeMap<OrderedKey, PendingEntry>(new Comparator<OrderedKey>() {
            @Override
            public int compare(OrderedKey left, OrderedKey right) {
                int result = schema.compare(left, right);
                if (result != 0) {
                    return result;
                }
                return left.getEncoded().compareTo(right.getEncoded());
            }
        });
    }

    public PendingEntry add(OrderedKeyGroup group, long estimatedGroupBytes) {
        PendingEntry entry = entries.get(group.getKey());
        if (entry == null) {
            entry = new PendingEntry(group.getKey());
            entries.put(group.getKey(), entry);
        }
        long before = entry.getEstimatedBytes();
        entry.add(group.getSourceId(), new LinkedHashMap<String, Object>(group.getFirstRow()), estimatedGroupBytes);
        estimatedBytes += Math.max(0, entry.getEstimatedBytes() - before);
        return entry;
    }

    public PendingEntry firstEntry() {
        Map.Entry<OrderedKey, PendingEntry> entry = entries.firstEntry();
        return entry != null ? entry.getValue() : null;
    }

    public PendingEntry remove(OrderedKey key) {
        PendingEntry removed = entries.remove(key);
        if (removed != null) {
            estimatedBytes -= removed.getEstimatedBytes();
        }
        return removed;
    }

    public List<PendingEntry> removeOldestUntilBelow(long maxBytes, int maxKeys) {
        List<PendingEntry> removed = new ArrayList<PendingEntry>();
        while (!entries.isEmpty() && (estimatedBytes > maxBytes || entries.size() > maxKeys)) {
            PendingEntry entry = firstEntry();
            if (entry == null) {
                break;
            }
            removed.add(remove(entry.getKey()));
        }
        return removed;
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public int size() {
        return entries.size();
    }

    public long getEstimatedBytes() {
        return estimatedBytes;
    }
}
