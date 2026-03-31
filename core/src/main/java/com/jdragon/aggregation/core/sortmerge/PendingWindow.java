package com.jdragon.aggregation.core.sortmerge;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 按 key 聚合的等待窗口。
 *
 * <p>窗口不再依赖“最小 key 可判定”，而是仅缓存每个 key 当前已到达的 source 首行。
 * entry 使用首次出现顺序维护，窗口超限时优先溢写最早进入但仍未完成的 key。
 */
public class PendingWindow {

    public static class PendingEntry {
        private final OrderedKey key;
        private final String encodedKey;
        private final long firstSeenSequence;
        private final Map<String, Map<String, Object>> firstRowsBySource =
                new LinkedHashMap<String, Map<String, Object>>();
        private long estimatedBytes;

        PendingEntry(OrderedKey key, long firstSeenSequence) {
            this.key = key;
            this.encodedKey = key.getEncoded();
            this.firstSeenSequence = firstSeenSequence;
        }

        public OrderedKey getKey() {
            return key;
        }

        public String getEncodedKey() {
            return encodedKey;
        }

        public long getFirstSeenSequence() {
            return firstSeenSequence;
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

    private final LinkedHashMap<String, PendingEntry> entries = new LinkedHashMap<String, PendingEntry>();
    private long estimatedBytes;
    private long sequenceGenerator;

    public PendingEntry add(OrderedKeyGroup group, long estimatedGroupBytes) {
        String encodedKey = group.getKey().getEncoded();
        PendingEntry entry = entries.get(encodedKey);
        if (entry == null) {
            entry = new PendingEntry(group.getKey(), ++sequenceGenerator);
            entries.put(encodedKey, entry);
        }
        long before = entry.getEstimatedBytes();
        entry.add(group.getSourceId(), new LinkedHashMap<String, Object>(group.getFirstRow()), estimatedGroupBytes);
        estimatedBytes += Math.max(0L, entry.getEstimatedBytes() - before);
        return entry;
    }

    public PendingEntry remove(OrderedKey key) {
        return key == null ? null : remove(key.getEncoded());
    }

    public PendingEntry remove(String encodedKey) {
        PendingEntry removed = entries.remove(encodedKey);
        if (removed != null) {
            estimatedBytes -= removed.getEstimatedBytes();
        }
        return removed;
    }

    public List<PendingEntry> removeOldestUntilBelow(long maxBytes, int maxKeys) {
        List<PendingEntry> removed = new ArrayList<PendingEntry>();
        while (!entries.isEmpty() && (estimatedBytes > maxBytes || entries.size() > maxKeys)) {
            Map.Entry<String, PendingEntry> oldest = entries.entrySet().iterator().next();
            PendingEntry removedEntry = remove(oldest.getKey());
            if (removedEntry == null) {
                break;
            }
            removed.add(removedEntry);
        }
        return removed;
    }

    public List<PendingEntry> snapshotEntries() {
        return new ArrayList<PendingEntry>(entries.values());
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
