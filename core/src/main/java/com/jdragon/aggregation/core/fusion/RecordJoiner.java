package com.jdragon.aggregation.core.fusion;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;

import java.util.*;

/**
 * 记录连接器
 * 基于连接键将多个数据源的记录进行连接
 */
public class RecordJoiner {
    
    private final List<String> joinKeys;
    private final String joinType;
    private final JoinCache joinCache;
    
    public RecordJoiner(List<String> joinKeys, String joinType, int cacheSize) {
        this.joinKeys = joinKeys;
        this.joinType = joinType.toUpperCase();
        this.joinCache = new JoinCache(cacheSize);
    }
    
    /**
     * 执行多源记录连接
     * @param sourceRecords 源记录映射（sourceId -> 记录列表）
     * @return 连接后的记录列表
     */
    public List<JoinedRecord> join(Map<String, List<Record>> sourceRecords) {
        if (sourceRecords.isEmpty()) {
            return Collections.emptyList();
        }
        
        // 构建每个数据源的连接键索引
        Map<String, Map<JoinKey, List<Record>>> sourceIndexes = new HashMap<>();
        for (Map.Entry<String, List<Record>> entry : sourceRecords.entrySet()) {
            String sourceId = entry.getKey();
            List<Record> records = entry.getValue();
            Map<JoinKey, List<Record>> index = joinCache.getIndex(sourceId, records, joinKeys);
            sourceIndexes.put(sourceId, index);
        }
        
        // 收集所有连接键
        Set<JoinKey> allKeys = new HashSet<>();
        for (Map<JoinKey, List<Record>> index : sourceIndexes.values()) {
            allKeys.addAll(index.keySet());
        }
        
        // 根据连接类型执行连接
        List<JoinedRecord> joinedRecords = new ArrayList<>();
        for (JoinKey key : allKeys) {
            Map<String, Record> matchedRecords = new HashMap<>();
            
            // 为每个数据源查找匹配记录
            for (Map.Entry<String, Map<JoinKey, List<Record>>> entry : sourceIndexes.entrySet()) {
                String sourceId = entry.getKey();
                Map<JoinKey, List<Record>> index = entry.getValue();
                List<Record> records = index.get(key);
                
                if (records != null && !records.isEmpty()) {
                    // 如果有多个匹配记录，取第一个（后续可支持一对多）
                    matchedRecords.put(sourceId, records.get(0));
                } else {
                    matchedRecords.put(sourceId, null);
                }
            }
            
            // 根据连接类型决定是否输出
            if (shouldOutput(matchedRecords)) {
                JoinedRecord joinedRecord = new JoinedRecord(key, matchedRecords);
                joinedRecords.add(joinedRecord);
            }
        }
        
        return joinedRecords;
    }
    
    /**
     * 根据连接类型判断是否输出
     */
    private boolean shouldOutput(Map<String, Record> matchedRecords) {
        List<String> sourceIds = new ArrayList<>(matchedRecords.keySet());
        if (sourceIds.isEmpty()) {
            return false;
        }
        
        switch (joinType) {
            case "INNER":
                // 所有数据源都必须有记录
                return matchedRecords.values().stream().allMatch(Objects::nonNull);
            case "LEFT":
                // 第一个数据源必须有记录
                return matchedRecords.get(sourceIds.get(0)) != null;
            case "RIGHT":
                // 最后一个数据源必须有记录
                return matchedRecords.get(sourceIds.get(sourceIds.size() - 1)) != null;
            case "FULL":
                // 至少一个数据源有记录
                return matchedRecords.values().stream().anyMatch(Objects::nonNull);
            default:
                return false;
        }
    }
    
    /**
     * 从记录中提取连接键
     */
    public JoinKey extractJoinKey(Record record) {
        List<Column> keyColumns = new ArrayList<>();
        for (String joinKey : joinKeys) {
            // 简化：假设记录中的列位置与字段名有对应关系
            // 实际应使用Record的字段名查找功能
            // 暂时使用列索引
            int index = getColumnIndex(record, joinKey);
            if (index >= 0) {
                keyColumns.add(record.getColumn(index));
            } else {
                keyColumns.add(null);
            }
        }
        return new JoinKey(keyColumns);
    }
    
    /**
     * 获取字段名对应的列索引（简化实现）
     */
    private int getColumnIndex(Record record, String fieldName) {
        // 实际实现需要Record支持字段名到索引的映射
        // 暂时返回-1
        return -1;
    }
    
    /**
     * 为记录列表构建连接键索引（静态工具方法）
     */
    public static Map<JoinKey, List<Record>> buildIndex(List<Record> records, List<String> joinKeys) {
        RecordJoiner joiner = new RecordJoiner(joinKeys, "INNER", 0);
        Map<JoinKey, List<Record>> index = new HashMap<>();
        
        for (Record record : records) {
            JoinKey key = joiner.extractJoinKey(record);
            index.computeIfAbsent(key, k -> new ArrayList<>()).add(record);
        }
        
        return index;
    }
    
    /**
     * 连接键
     */
    public static class JoinKey {
        private final List<Column> columns;
        
        public JoinKey(List<Column> columns) {
            this.columns = columns;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            JoinKey joinKey = (JoinKey) o;
            if (columns.size() != joinKey.columns.size()) return false;
            for (int i = 0; i < columns.size(); i++) {
                Column c1 = columns.get(i);
                Column c2 = joinKey.columns.get(i);
                if (c1 == null && c2 == null) continue;
                if (c1 == null || c2 == null) return false;
                if (!c1.equals(c2)) return false;
            }
            return true;
        }
        
        @Override
        public int hashCode() {
            int result = 1;
            for (Column column : columns) {
                result = 31 * result + (column == null ? 0 : column.hashCode());
            }
            return result;
        }
    }
    
    /**
     * 连接后的记录
     */
    public static class JoinedRecord {
        private final JoinKey joinKey;
        private final Map<String, Record> sourceRecords;
        
        public JoinedRecord(JoinKey joinKey, Map<String, Record> sourceRecords) {
            this.joinKey = joinKey;
            this.sourceRecords = sourceRecords;
        }
        
        public JoinKey getJoinKey() {
            return joinKey;
        }
        
        public Map<String, Record> getSourceRecords() {
            return sourceRecords;
        }
        
        public Record getRecord(String sourceId) {
            return sourceRecords.get(sourceId);
        }
    }
}