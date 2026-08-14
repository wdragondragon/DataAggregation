package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.transfer.model.TransferMapping;
import com.jdragon.aggregation.transfer.model.TransferPolicy;
import com.jdragon.aggregation.transfer.model.TransferSelection;
import com.jdragon.aggregation.transfer.model.TransferSpec;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DynamicTemplateResolver {

    private static final Pattern EXPRESSION = Pattern.compile("\\$\\{([^}]+)}");

    public TransferSpec resolve(TransferSpec spec, String runId, Instant plannedAt) {
        ZoneId zoneId = ZoneId.of(spec.timeZone());
        Map<String, String> parameters = spec.parameters();
        TransferSelection selection = spec.selection();
        TransferMapping mapping = spec.mapping();
        TransferPolicy policy = spec.policy();

        TransferSelection resolvedSelection = new TransferSelection(
                resolve(selection.rootPath(), runId, plannedAt, zoneId, parameters),
                resolveList(selection.paths(), runId, plannedAt, zoneId, parameters),
                selection.recursive(),
                resolveList(selection.includeGlobs(), runId, plannedAt, zoneId, parameters),
                resolve(selection.includeRegex(), runId, plannedAt, zoneId, parameters),
                resolveList(selection.excludeGlobs(), runId, plannedAt, zoneId, parameters),
                selection.minSize(), selection.maxSize(), selection.modifiedAfterMillis(),
                selection.modifiedBeforeMillis(), selection.maxFiles());
        TransferMapping resolvedMapping = new TransferMapping(
                resolve(mapping.targetRootPath(), runId, plannedAt, zoneId, parameters),
                mapping.preserveRelativePath(),
                resolve(mapping.targetPathTemplate(), runId, plannedAt, zoneId, parameters));
        TransferPolicy resolvedPolicy = new TransferPolicy(policy.conflictPolicy(), policy.checksumAlgorithm(),
                policy.verificationMode(), policy.verificationFrameCount(),
                policy.verificationFrameSizeBytes(), policy.sourceSuccessAction(),
                resolve(policy.sourceBackupRootPath(), runId, plannedAt, zoneId, parameters));
        return new TransferSpec(spec.schemaVersion(), spec.source(), spec.target(), resolvedSelection,
                resolvedMapping, resolvedPolicy, spec.runtime(), spec.timeZone(), spec.parameters());
    }

    public String resolve(String value, String runId, Instant plannedAt, ZoneId zoneId,
                          Map<String, String> parameters) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        Matcher matcher = EXPRESSION.matcher(value);
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String replacement = expressionValue(matcher.group(1), runId, plannedAt, zoneId, parameters);
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private List<String> resolveList(List<String> values, String runId, Instant plannedAt,
                                     ZoneId zoneId, Map<String, String> parameters) {
        return values.stream().map(value -> resolve(value, runId, plannedAt, zoneId, parameters)).toList();
    }

    private String expressionValue(String expression, String runId, Instant plannedAt,
                                   ZoneId zoneId, Map<String, String> parameters) {
        if ("runId".equals(expression)) {
            return runId;
        }
        if ("plannedAt".equals(expression)) {
            return plannedAt.toString();
        }
        if ("relativePath".equals(expression) || "fileName".equals(expression)) {
            return "${" + expression + "}";
        }
        if (expression.startsWith("now:") || expression.startsWith("date:")) {
            String pattern = expression.substring(expression.indexOf(':') + 1);
            return DateTimeFormatter.ofPattern(pattern).withZone(zoneId).format(plannedAt);
        }
        if (expression.startsWith("param:") || expression.startsWith("parameter:")) {
            String key = expression.substring(expression.indexOf(':') + 1);
            if (!parameters.containsKey(key)) {
                throw new IllegalArgumentException("missing dynamic parameter: " + key);
            }
            return parameters.get(key);
        }
        if (expression.startsWith("parameter.")) {
            String key = expression.substring("parameter.".length());
            if (!parameters.containsKey(key)) {
                throw new IllegalArgumentException("missing dynamic parameter: " + key);
            }
            return parameters.get(key);
        }
        throw new IllegalArgumentException("unsupported dynamic expression: ${" + expression + "}");
    }
}
