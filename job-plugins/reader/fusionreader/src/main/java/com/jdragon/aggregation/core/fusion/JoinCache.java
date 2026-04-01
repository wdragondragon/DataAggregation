package com.jdragon.aggregation.core.fusion;

import com.jdragon.aggregation.commons.element.Record;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 连接缓存
 * 缓存数据源索引以提高连接性能
 */
public class JoinCache {
    
    private final int maxSize;
    private final Map<String, Map<RecordJoiner.JoinKey, List<Record>>> sourceIndexCache;
    private final LinkedHashMap<String, Long> accessOrder;
    
    public JoinCache(int maxSize) {
        this.maxSize = maxSize;
        this.sourceIndexCache = new ConcurrentHashMap<>();
        this.accessOrder = new LinkedHashMap<>(16, 0.75f, true);
    }
    
    /**
     * 获取数据源的连接键索引
     */
    public Map<RecordJoiner.JoinKey, List<Record>> getIndex(String sourceId, List<Record> records, List<String> joinKeys) {
        String cacheKey = generateCacheKey(sourceId, records, joinKeys);
        updateAccess(cacheKey);
        
        Map<RecordJoiner.JoinKey, List<Record>> index = sourceIndexCache.get(cacheKey);
        if (index == null) {
            index = buildIndex(records, joinKeys);
            putIndex(cacheKey, index);
        }
        
        return index;
    }
    
    /**
     * 构建索引
     */
    private Map<RecordJoiner.JoinKey, List<Record>> buildIndex(List<Record> records, List<String> joinKeys) {
        return RecordJoiner.buildIndex(records, joinKeys);
    }
    
    /**
     * 缓存索引
     */
    private void putIndex(String cacheKey, Map<RecordJoiner.JoinKey, List<Record>> index) {
        if (sourceIndexCache.size() >= maxSize) {
            // 移除最久未使用的条目
            Iterator<String> iterator = accessOrder.keySet().iterator();
            if (iterator.hasNext()) {
                String oldestKey = iterator.next();
                sourceIndexCache.remove(oldestKey);
                accessOrder.remove(oldestKey);
            }
        }
        
        sourceIndexCache.put(cacheKey, index);
        accessOrder.put(cacheKey, System.currentTimeMillis());
    }
    
    /**
     * 生成缓存键
     */
    private String generateCacheKey(String sourceId, List<Record> records, List<String> joinKeys) {
        // 简化实现：使用源ID、记录数量和连接键生成键
        int recordHash = records.hashCode();
        int joinKeyHash = joinKeys.hashCode();
        return sourceId + ":" + recordHash + ":" + joinKeyHash;
    }
    
    /**
     * 更新访问时间
     */
    private void updateAccess(String cacheKey) {
        accessOrder.put(cacheKey, System.currentTimeMillis());
    }
    
    /**
     * 清除缓存
     */
    public void clear() {
        sourceIndexCache.clear();
        accessOrder.clear();
    }
    
    /**
     * 获取缓存大小
     */
    public int size() {
        return sourceIndexCache.size();
    }
}