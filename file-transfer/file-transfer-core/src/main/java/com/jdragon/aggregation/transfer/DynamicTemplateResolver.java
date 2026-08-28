package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.commons.util.DynamicFunctionResolver;
import com.jdragon.aggregation.transfer.model.TransferMapping;
import com.jdragon.aggregation.transfer.model.TransferPolicy;
import com.jdragon.aggregation.transfer.model.TransferSelection;
import com.jdragon.aggregation.transfer.model.TransferSpec;

import java.time.Instant;
import java.time.LocalDateTime;
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
        LocalDateTime functionBaseTime = LocalDateTime.ofInstant(plannedAt, zoneId);

        TransferSelection resolvedSelection = new TransferSelection(
                resolve(selection.rootPath(), runId, plannedAt, zoneId, parameters, functionBaseTime,
                        "selection.rootPath"),
                resolveList(selection.paths(), runId, plannedAt, zoneId, parameters, functionBaseTime,
                        "selection.paths"),
                selection.recursive(),
                resolveList(selection.includeGlobs(), runId, plannedAt, zoneId, parameters, functionBaseTime,
                        "selection.includeGlobs"),
                resolve(selection.includeRegex(), runId, plannedAt, zoneId, parameters, functionBaseTime,
                        "selection.includeRegex"),
                resolveList(selection.excludeGlobs(), runId, plannedAt, zoneId, parameters, functionBaseTime,
                        "selection.excludeGlobs"),
                selection.minSize(), selection.maxSize(), selection.modifiedAfterMillis(),
                selection.modifiedBeforeMillis(), selection.maxFiles());
        TransferMapping resolvedMapping = new TransferMapping(
                resolve(mapping.targetRootPath(), runId, plannedAt, zoneId, parameters, functionBaseTime,
                        "mapping.targetRootPath"),
                mapping.preserveRelativePath(),
                resolve(mapping.targetPathTemplate(), runId, plannedAt, zoneId, parameters, functionBaseTime,
                        "mapping.targetPathTemplate"));
        TransferPolicy resolvedPolicy = new TransferPolicy(policy.conflictPolicy(), policy.checksumAlgorithm(),
                policy.verificationMode(), policy.verificationFrameCount(),
                policy.verificationFrameSizeBytes(), policy.sourceSuccessAction(),
                resolveTemplatesOnly(policy.sourceBackupRootPath(), runId, plannedAt, zoneId, parameters));
        return new TransferSpec(spec.schemaVersion(), spec.source(), spec.target(), resolvedSelection,
                resolvedMapping, resolvedPolicy, spec.runtime(), spec.timeZone(), spec.parameters());
    }

    public String resolve(String value, String runId, Instant plannedAt, ZoneId zoneId,
                          Map<String, String> parameters) {
        return resolve(value, runId, plannedAt, zoneId, parameters,
                LocalDateTime.ofInstant(plannedAt, zoneId), "expression");
    }

    private String resolve(String value, String runId, Instant plannedAt, ZoneId zoneId,
                           Map<String, String> parameters, LocalDateTime functionBaseTime,
                           String field) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        validateTemplateSyntax(value, field);
        final String functionResolved;
        try {
            functionResolved = DynamicFunctionResolver.replaceAll(value, functionBaseTime,
                    DynamicFunctionResolver.Mode.STRICT, zoneId);
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException("Invalid dynamic function in " + field + ": "
                    + value + "; " + exception.getMessage(), exception);
        }
        Matcher matcher = EXPRESSION.matcher(functionResolved);
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String replacement = expressionValue(matcher.group(1), runId, plannedAt, zoneId, parameters,
                    "mapping.targetPathTemplate".equals(field));
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private String resolveTemplatesOnly(String value, String runId, Instant plannedAt, ZoneId zoneId,
                                        Map<String, String> parameters) {
        if (value == null || value.isEmpty()) {
            return value;
        }
        validateTemplateSyntax(value, "expression");
        Matcher matcher = EXPRESSION.matcher(value);
        StringBuffer result = new StringBuffer();
        while (matcher.find()) {
            String replacement = expressionValue(matcher.group(1), runId, plannedAt, zoneId,
                    parameters, false);
            matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private void validateTemplateSyntax(String value, String field) {
        int cursor = 0;
        while (cursor < value.length()) {
            int start = value.indexOf("${", cursor);
            if (start < 0) {
                return;
            }
            int end = value.indexOf('}', start + 2);
            if (end < 0) {
                throw new IllegalArgumentException("Invalid template in " + field
                        + ": unclosed expression starting at position " + start);
            }
            String body = value.substring(start + 2, end);
            if (body.isBlank()) {
                throw new IllegalArgumentException("Invalid template in " + field
                        + ": template variable name is required");
            }
            if (body.contains("${")) {
                throw new IllegalArgumentException("Invalid template in " + field
                        + ": nested template expressions are not supported");
            }
            if (body.matches(".*\\$[A-Za-z_][A-Za-z0-9_]*\\(.*")) {
                throw new IllegalArgumentException("Invalid template in " + field
                        + ": dynamic functions cannot be nested inside template variables");
            }
            cursor = end + 1;
        }
    }

    private List<String> resolveList(List<String> values, String runId, Instant plannedAt,
                                     ZoneId zoneId, Map<String, String> parameters,
                                     LocalDateTime functionBaseTime, String field) {
        List<String> resolved = new java.util.ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            resolved.add(resolve(values.get(index), runId, plannedAt, zoneId, parameters,
                    functionBaseTime, field + "[" + index + "]"));
        }
        return resolved;
    }

    private String expressionValue(String expression, String runId, Instant plannedAt,
                                   ZoneId zoneId, Map<String, String> parameters,
                                   boolean allowPerFileTemplates) {
        if ("runId".equals(expression)) {
            return runId;
        }
        if ("plannedAt".equals(expression)) {
            return plannedAt.toString();
        }
        if ("relativePath".equals(expression) || "fileName".equals(expression)) {
            if (!allowPerFileTemplates) {
                throw new IllegalArgumentException("${" + expression + "} is only supported in mapping.targetPathTemplate");
            }
            return "${" + expression + "}";
        }
        if (expression.startsWith("now:") || expression.startsWith("date:")) {
            String pattern = expression.substring(expression.indexOf(':') + 1);
            if (pattern.isBlank()) {
                throw new IllegalArgumentException("template time pattern is required");
            }
            try {
                DateTimeFormatter.ofPattern(pattern);
            } catch (RuntimeException exception) {
                throw new IllegalArgumentException("invalid template time pattern: " + pattern, exception);
            }
            return DateTimeFormatter.ofPattern(pattern).withZone(zoneId).format(plannedAt);
        }
        if (expression.startsWith("param:") || expression.startsWith("parameter:")) {
            String key = expression.substring(expression.indexOf(':') + 1);
            if (!parameters.containsKey(key)) {
                throw new IllegalArgumentException("missing dynamic parameter: " + key);
            }
            return requireParameterValue(key, parameters.get(key));
        }
        if (expression.startsWith("parameter.")) {
            String key = expression.substring("parameter.".length());
            if (!parameters.containsKey(key)) {
                throw new IllegalArgumentException("missing dynamic parameter: " + key);
            }
            return requireParameterValue(key, parameters.get(key));
        }
        throw new IllegalArgumentException("unsupported dynamic expression: ${" + expression + "}");
    }

    private String requireParameterValue(String key, String value) {
        if (value == null) {
            throw new IllegalArgumentException("dynamic parameter is null: " + key);
        }
        return value;
    }
}
