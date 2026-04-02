package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.i18n.MessageResource;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;

import java.util.*;

@Slf4j
public class DataComparator {

    private final double toleranceThreshold;

    private final List<String> compareFields;

    private final MessageResource messages;

    public DataComparator(double toleranceThreshold, List<String> compareFields) {
        this(toleranceThreshold, compareFields, MessageResource.forLanguage(com.jdragon.aggregation.core.consistency.model.OutputConfig.ReportLanguage.ENGLISH));
    }

    public DataComparator(double toleranceThreshold, List<String> compareFields, MessageResource messages) {
        this.toleranceThreshold = toleranceThreshold;
        this.compareFields = compareFields != null ? compareFields : Collections.emptyList();
        this.messages = messages != null ? messages : MessageResource.forLanguage(com.jdragon.aggregation.core.consistency.model.OutputConfig.ReportLanguage.ENGLISH);
    }

    public List<DifferenceRecord> compareData(
            Map<String, Map<String, List<Map<String, Object>>>> groupedData,
            List<String> matchKeys) {

        List<DifferenceRecord> differences = new ArrayList<>();
        LinkedHashSet<String> allMatchKeys = collectAllMatchKeys(groupedData);
        List<String> sourceIds = new ArrayList<>(groupedData.keySet());

        for (String matchKey : allMatchKeys) {
            Map<String, Map<String, Object>> keyGroup = new LinkedHashMap<>();
            for (String sourceId : sourceIds) {
                Map<String, List<Map<String, Object>>> sourceGroup = groupedData.get(sourceId);
                if (sourceGroup == null) {
                    continue;
                }
                List<Map<String, Object>> rows = sourceGroup.get(matchKey);
                if (rows != null && !rows.isEmpty()) {
                    keyGroup.put(sourceId, rows.get(0));
                }
            }

            DifferenceRecord diffRecord = compareKeyGroup(matchKey, matchKeys, sourceIds, keyGroup);
            if (diffRecord != null) {
                differences.add(diffRecord);
            }
        }

        log.info("Found {} differences across {} match keys", differences.size(), allMatchKeys.size());
        return differences;
    }

    public DifferenceRecord compareKeyGroup(
            String matchKey,
            List<String> matchKeyFields,
            List<String> sourceOrder,
            Map<String, Map<String, Object>> firstRowsBySource) {

        DifferenceRecord diffRecord = new DifferenceRecord();
        diffRecord.setRecordId(UUID.randomUUID().toString());
        diffRecord.setMatchKeyValues(parseMatchKeyValues(matchKey, matchKeyFields));

        Map<String, Map<String, Object>> sourceValues = new LinkedHashMap<>();
        List<String> missingSources = new ArrayList<>();

        for (String sourceId : sourceOrder) {
            Map<String, Object> record = firstRowsBySource != null ? firstRowsBySource.get(sourceId) : null;
            if (record != null) {
                sourceValues.put(sourceId, record);
            } else {
                Map<String, Object> missingRecord = createMissingRecord(matchKey, matchKeyFields);
                for (String field : compareFields) {
                    missingRecord.put(field, null);
                }
                missingSources.add(sourceId);
                sourceValues.put(sourceId, missingRecord);
            }
        }

        diffRecord.setSourceValues(sourceValues);

        boolean hasDifferences = false;
        if (!missingSources.isEmpty()) {
            diffRecord.setMissingSources(missingSources);
            hasDifferences = true;
        }

        if (sourceValues.size() > 1) {
            for (String field : compareFields) {
                String difference = compareFieldAcrossSources(field, sourceValues);
                if (difference != null) {
                    diffRecord.addDifference(field, difference);
                    hasDifferences = true;
                }
            }
        }

        if (!hasDifferences) {
            return null;
        }

        diffRecord.setConflictType(determineConflictType(sourceValues, missingSources));
        diffRecord.setDiscrepancyScore(calculateDiscrepancyScore(diffRecord.getDifferences(), sourceValues, missingSources.size()));
        return diffRecord;
    }

    private LinkedHashSet<String> collectAllMatchKeys(Map<String, Map<String, List<Map<String, Object>>>> groupedData) {
        LinkedHashSet<String> allMatchKeys = new LinkedHashSet<>();

        if (groupedData.isEmpty()) {
            return allMatchKeys;
        }

        String orderingSourceId = null;
        Map<String, List<Map<String, Object>>> orderingSourceGroup = null;

        for (Map.Entry<String, Map<String, List<Map<String, Object>>>> entry : groupedData.entrySet()) {
            Map<String, List<Map<String, Object>>> sourceGroup = entry.getValue();
            if (sourceGroup != null && !sourceGroup.isEmpty()) {
                orderingSourceId = entry.getKey();
                orderingSourceGroup = sourceGroup;
                break;
            }
        }

        if (orderingSourceGroup != null) {
            allMatchKeys.addAll(orderingSourceGroup.keySet());
        }

        for (Map.Entry<String, Map<String, List<Map<String, Object>>>> entry : groupedData.entrySet()) {
            String sourceId = entry.getKey();
            if (orderingSourceId != null && sourceId.equals(orderingSourceId)) {
                continue;
            }
            Map<String, List<Map<String, Object>>> sourceGroup = entry.getValue();
            if (sourceGroup != null) {
                for (String matchKey : sourceGroup.keySet()) {
                    if (!allMatchKeys.contains(matchKey)) {
                        allMatchKeys.add(matchKey);
                    }
                }
            }
        }

        return allMatchKeys;
    }

    private Map<String, Object> createMissingRecord(String matchKey, List<String> matchKeyFields) {
        Map<String, Object> values = new LinkedHashMap<>();
        if (matchKeyFields == null || matchKeyFields.isEmpty()) {
            values.put("key", matchKey);
            return values;
        }

        String[] parts = matchKey.split("-\\|-", -1);
        for (int i = 0; i < matchKeyFields.size(); i++) {
            values.put(matchKeyFields.get(i), i < parts.length ? parts[i] : null);
        }
        return values;
    }

    private Map<String, Object> parseMatchKeyValues(String matchKey, List<String> matchKeyFields) {
        Map<String, Object> values = new HashMap<>();
        if (matchKeyFields == null || matchKeyFields.isEmpty()) {
            values.put("key", matchKey);
            return values;
        }

        String[] parts = matchKey.split("-\\|-", -1);
        for (int i = 0; i < Math.min(parts.length, matchKeyFields.size()); i++) {
            values.put(matchKeyFields.get(i), parts[i]);
        }

        return values;
    }

    private String compareFieldAcrossSources(String field, Map<String, Map<String, Object>> sourceValues) {
        Set<String> sourcesWithNull = new HashSet<>();
        Map<String, Object> sourceValue = new LinkedHashMap<>();
        boolean allSame = true;
        Object firstNotNull = ObjectUtils.NULL;
        for (Map.Entry<String, Map<String, Object>> entry : sourceValues.entrySet()) {
            String sourceId = entry.getKey();
            Map<String, Object> record = entry.getValue();
            Object value = record.get(field);
            sourceValue.put(sourceId, value);
            if (value == null) {
                sourcesWithNull.add(sourceId);
            }
            if (firstNotNull == ObjectUtils.NULL) {
                firstNotNull = value;
            }
            if (!Objects.equals(firstNotNull, value)) {
                allSame = false;
            }
        }

        if (sourceValue.size() == 1 && sourcesWithNull.isEmpty()) {
            return null;
        }

        if (allSame) {
            return null;
        }

        return messages.getMessage("comparison.different.values", field);
    }

    private boolean isNumericField(Set<Object> values) {
        for (Object value : values) {
            if (value == null) {
                continue;
            }
            if (value instanceof Number) {
                continue;
            }
            try {
                Double.parseDouble(value.toString());
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return !values.isEmpty();
    }

    private String compareNumericField(String field, Map<String, Object> sourceToValue) {
        List<Double> numericValues = new ArrayList<>();
        Map<String, Double> sourceNumericValues = new HashMap<>();

        for (Map.Entry<String, Object> entry : sourceToValue.entrySet()) {
            String sourceId = entry.getKey();
            Object value = entry.getValue();
            if (value == null) {
                continue;
            }

            double numValue;
            if (value instanceof Number) {
                numValue = ((Number) value).doubleValue();
            } else {
                try {
                    numValue = Double.parseDouble(value.toString());
                } catch (NumberFormatException e) {
                    return messages.getMessage("comparison.mixed.numeric", field, sourceToValue.values());
                }
            }
            numericValues.add(numValue);
            sourceNumericValues.put(sourceId, numValue);
        }

        if (numericValues.isEmpty()) {
            return null;
        }

        Collections.sort(numericValues);
        double min = numericValues.get(0);
        double max = numericValues.get(numericValues.size() - 1);
        double range = max - min;

        if (range <= toleranceThreshold) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        sb.append(field).append(": {");
        boolean first = true;
        for (Map.Entry<String, Object> entry : sourceToValue.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(entry.getKey()).append(": ");
            if (entry.getValue() == null) {
                sb.append("null");
            } else {
                sb.append(entry.getValue());
            }
            first = false;
        }
        sb.append("}, range: ").append(range).append(" > tolerance: ").append(toleranceThreshold);
        return sb.toString();
    }

    private String determineConflictType(Map<String, Map<String, Object>> sourceValues, List<String> missingSources) {
        int sourceCount = sourceValues.size();

        if (!missingSources.isEmpty()) {
            return "MISSING";
        } else if (sourceCount == 2) {
            return "BINARY_CONFLICT";
        } else if (sourceCount > 2) {
            return "MULTI_SOURCE_CONFLICT";
        }
        return "UNKNOWN";
    }

    private double calculateDiscrepancyScore(Map<String, String> differences, Map<String, Map<String, Object>> sourceValues, int missingSourceCount) {
        if (differences.isEmpty()) {
            return 0.0;
        }

        double fieldScore = differences.size() / (double) Math.max(compareFields.size(), 1);

        int totalSourcesConsidered = sourceValues.size() + missingSourceCount;
        double sourceScore = 0.0;
        if (totalSourcesConsidered > 0) {
            sourceScore = 1.0 - (sourceValues.size() / (double) totalSourcesConsidered);
        }

        return (fieldScore + sourceScore) / 2.0;
    }
}
