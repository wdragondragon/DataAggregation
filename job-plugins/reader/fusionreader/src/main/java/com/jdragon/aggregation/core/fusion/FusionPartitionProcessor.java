package com.jdragon.aggregation.core.fusion;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import com.jdragon.aggregation.core.plugin.RecordSender;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class FusionPartitionProcessor {

    private final FusionConfig fusionConfig;
    private final FusionContext fusionContext;
    private final RecordSender recordSender;
    private final FusionEngine fusionEngine;
    private final Method fuseRowsMethod;
    private final List<String> sourceOrder;

    FusionPartitionProcessor(FusionConfig fusionConfig, FusionContext fusionContext, RecordSender recordSender) {
        this.fusionConfig = fusionConfig;
        this.fusionContext = fusionContext;
        this.recordSender = recordSender;
        this.fusionEngine = new FusionEngine(fusionConfig, fusionContext);
        this.sourceOrder = fusionConfig.getSources().stream()
                .map(com.jdragon.aggregation.core.fusion.config.SourceConfig::getSourceId)
                .collect(Collectors.toList());
        try {
            this.fuseRowsMethod = FusionEngine.class.getDeclaredMethod("fuseRows", Map.class);
            this.fuseRowsMethod.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("FusionEngine#fuseRows is unavailable", e);
        }
    }

    void processGroups(Map<String, LinkedHashMap<String, Map<String, Object>>> groups) {
        for (Map.Entry<String, LinkedHashMap<String, Map<String, Object>>> entry : groups.entrySet()) {
            String joinKey = entry.getKey();
            Map<String, Map<String, Object>> sourceRows = entry.getValue();
            fusionContext.setCurrentJoinKey(joinKey);

            if (!shouldEmit(sourceRows)) {
                fusionContext.recordSkipped();
                continue;
            }

            try {
                Record record = (Record) fuseRowsMethod.invoke(fusionEngine, sourceRows);
                if (record == null) {
                    fusionContext.recordSkipped();
                    continue;
                }
                recordSender.sendToWriter(record);
                fusionContext.recordProcessed();
            } catch (IllegalAccessException e) {
                throw new IllegalStateException("Unable to access FusionEngine#fuseRows", e);
            } catch (InvocationTargetException e) {
                Throwable target = e.getTargetException();
                Exception exception = target instanceof Exception
                        ? (Exception) target
                        : new RuntimeException(target);
                fusionContext.handleFieldErrorWithJoinKey(joinKey, "_record", target.getMessage(), exception);
                fusionContext.recordSkipped();
            }
        }
    }

    private boolean shouldEmit(Map<String, Map<String, Object>> sourceRows) {
        if (sourceOrder.isEmpty()) {
            return false;
        }

        switch (fusionConfig.getJoinType()) {
            case INNER:
                return sourceOrder.stream().allMatch(sourceRows::containsKey);
            case LEFT:
                return sourceRows.containsKey(sourceOrder.get(0));
            case RIGHT:
                return sourceRows.containsKey(sourceOrder.get(sourceOrder.size() - 1));
            case FULL:
                return sourceOrder.stream().anyMatch(sourceRows::containsKey);
            default:
                return false;
        }
    }
}
