package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;
import com.jdragon.aggregation.core.consistency.model.UpdatePlan;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class LegacyUpdatePlanningSupport {

    private LegacyUpdatePlanningSupport() {
    }

    static UpdateExecutor.OperationType determineOperationType(DifferenceRecord diff, String targetSourceId) {
        UpdateExecutor.OperationType explicitOperationType = resolveExplicitOperationType(diff);
        if (explicitOperationType != null) {
            return explicitOperationType;
        }

        Set<String> missingSources = new HashSet<>(diff.getMissingSources() != null
                ? diff.getMissingSources()
                : Collections.emptyList());

        boolean recordShouldExist = false;
        if (diff.getResolutionResult() != null) {
            String winningSource = diff.getResolutionResult().getWinningSource();
            if (winningSource != null && !missingSources.contains(winningSource)) {
                recordShouldExist = true;
            } else if (winningSource != null && hasPresentSource(diff) && hasMeaningfulResolvedValues(diff)) {
                recordShouldExist = true;
            } else {
                recordShouldExist = hasNonMatchKeyValues(diff);
            }
        }

        boolean targetExists = !missingSources.contains(targetSourceId);
        if (recordShouldExist && !targetExists) {
            return UpdateExecutor.OperationType.INSERT;
        }
        if (!recordShouldExist && targetExists) {
            return UpdateExecutor.OperationType.DELETE;
        }
        return UpdateExecutor.OperationType.UPDATE;
    }

    static UpdateExecutor.OperationType resolveExplicitOperationType(DifferenceRecord diff) {
        String operationType = rawExplicitOperationType(diff);
        if (operationType == null || operationType.trim().isEmpty()) {
            return null;
        }
        try {
            return UpdateExecutor.OperationType.valueOf(operationType.trim().toUpperCase());
        } catch (IllegalArgumentException ex) {
            return null;
        }
    }

    static String rawExplicitOperationType(DifferenceRecord diff) {
        if (diff == null) {
            return null;
        }
        UpdatePlan updatePlan = diff.getUpdatePlan();
        String operationType = updatePlan != null ? updatePlan.getOperationType() : null;
        if ((operationType == null || operationType.trim().isEmpty()) && diff.getResolutionResult() != null) {
            operationType = diff.getResolutionResult().getOperationType();
        }
        if (operationType == null || operationType.trim().isEmpty()) {
            return null;
        }
        return operationType;
    }

    static boolean hasNonMatchKeyValues(DifferenceRecord diff) {
        Map<String, Object> resolvedValues = effectiveResolvedValues(diff);
        if (resolvedValues == null || resolvedValues.isEmpty()) {
            return false;
        }
        Set<String> matchKeysSet = new HashSet<>(effectiveMatchKeyValues(diff).keySet());
        for (Map.Entry<String, Object> entry : resolvedValues.entrySet()) {
            if (!matchKeysSet.contains(entry.getKey()) && entry.getValue() != null) {
                return true;
            }
        }
        return false;
    }

    static boolean hasMeaningfulResolvedValues(DifferenceRecord diff) {
        Map<String, Object> resolvedValues = effectiveResolvedValues(diff);
        if (resolvedValues == null || resolvedValues.isEmpty()) {
            return false;
        }
        for (Object value : resolvedValues.values()) {
            if (value != null) {
                return true;
            }
        }
        return false;
    }

    static boolean hasPresentSource(DifferenceRecord diff) {
        if (diff == null || diff.getSourceValues() == null || diff.getSourceValues().isEmpty()) {
            return false;
        }
        Set<String> missingSources = new HashSet<>(diff.getMissingSources() != null
                ? diff.getMissingSources()
                : Collections.emptyList());
        for (Map.Entry<String, Map<String, Object>> entry : diff.getSourceValues().entrySet()) {
            if (!missingSources.contains(entry.getKey())) {
                return true;
            }
        }
        return false;
    }

    static Map<String, Object> effectiveResolvedValues(DifferenceRecord diff) {
        if (diff != null && diff.getUpdatePlan() != null && diff.getUpdatePlan().getResolvedValues() != null) {
            return diff.getUpdatePlan().getResolvedValues();
        }
        ResolutionResult resolutionResult = diff != null ? diff.getResolutionResult() : null;
        return resolutionResult != null ? resolutionResult.getResolvedValues() : null;
    }

    static Map<String, Object> effectiveMatchKeyValues(DifferenceRecord diff) {
        if (diff != null && diff.getUpdatePlan() != null && diff.getUpdatePlan().getMatchKeyValues() != null
                && !diff.getUpdatePlan().getMatchKeyValues().isEmpty()) {
            return diff.getUpdatePlan().getMatchKeyValues();
        }
        return diff != null && diff.getMatchKeyValues() != null ? diff.getMatchKeyValues() : Collections.emptyMap();
    }
}
