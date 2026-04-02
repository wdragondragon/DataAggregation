package com.jdragon.aggregation.core.consistency;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;

import java.util.List;

public class ConsistencyRecordProjector {

    public Record project(String ruleId, DifferenceRecord differenceRecord, List<String> targetColumns) {
        DefaultRecord record = new DefaultRecord();
        String payload = JSONObject.toJSONString(differenceRecord);

        if (targetColumns == null || targetColumns.isEmpty()) {
            record.addColumn(new StringColumn(payload));
            return record;
        }

        for (String targetColumn : targetColumns) {
            record.addColumn(new StringColumn(resolveValue(ruleId, differenceRecord, targetColumn, payload)));
        }
        return record;
    }

    private String resolveValue(String ruleId, DifferenceRecord differenceRecord, String targetColumn, String payload) {
        if (targetColumn == null) {
            return payload;
        }
        String normalized = targetColumn.trim().toLowerCase();
        switch (normalized) {
            case "ruleid":
            case "rule_id":
                return ruleId;
            case "recordid":
            case "record_id":
                return differenceRecord.getRecordId();
            case "matchkeys":
            case "match_keys":
            case "matchkeyvalues":
                return JSONObject.toJSONString(differenceRecord.getMatchKeyValues());
            case "differences":
                return JSONObject.toJSONString(differenceRecord.getDifferences());
            case "conflicttype":
            case "conflict_type":
                return differenceRecord.getConflictType();
            case "payload":
            default:
                return payload;
        }
    }
}
