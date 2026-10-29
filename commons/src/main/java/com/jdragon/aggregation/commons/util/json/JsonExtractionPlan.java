package com.jdragon.aggregation.commons.util.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.jdragon.aggregation.commons.util.json.JsonRecordExtractor.CompiledProjection;
import static com.jdragon.aggregation.commons.util.json.JsonRecordExtractor.PathToken;
import static com.jdragon.aggregation.commons.util.json.JsonRecordExtractor.TokenKind;

/** Immutable compiled paths; each extraction has its own array bindings and output state. */
public final class JsonExtractionPlan {
    private final String recordPath;
    private final List<PathToken> recordTokens;
    private final List<CompiledProjection> projections;

    JsonExtractionPlan(String recordPath, List<PathToken> recordTokens, List<CompiledProjection> projections) {
        this.recordPath = recordPath;
        this.recordTokens = Collections.unmodifiableList(new ArrayList<>(recordTokens));
        this.projections = Collections.unmodifiableList(new ArrayList<>(projections));
    }

    /**
     * Extracts object records in array order, stopping at {@code maxRows} when it is positive.
     * Missing or null record nodes use INNER semantics and produce no rows. The source count is
     * the first wildcard array size, or the object record count (zero or one) without wildcards.
     */
    public JsonExtractionResult extract(JSON response, int maxRows) {
        JSONArray rows = new JSONArray();
        ExtractionState state = new ExtractionState(response, rows, maxRows);
        traverse(response, 0, 0, new HashMap<Integer, Integer>(), state);
        int sourceRowCount = state.sourceRowCountSet
                ? state.sourceRowCount
                : rows.size();
        return new JsonExtractionResult(rows, sourceRowCount);
    }

    private void traverse(Object current,
                          int tokenIndex,
                          int wildcardIndex,
                          Map<Integer, Integer> bindings,
                          ExtractionState state) {
        if (state.isAtLimit()) {
            return;
        }
        if (tokenIndex >= recordTokens.size()) {
            if (current == null) {
                return;
            }
            if (!(current instanceof JSONObject)) {
                throw typeMismatch(recordPath, "object", current);
            }
            JSONObject row = new JSONObject(true);
            for (CompiledProjection projection : projections) {
                Object parent = resolveProjectionParent(state.response, projection.parentTokens,
                        projection.outputName, bindings);
                Object value = parent instanceof JSONObject
                        ? ((JSONObject) parent).get(projection.sourceName)
                        : null;
                row.put(projection.outputName, value);
            }
            state.rows.add(row);
            return;
        }

        PathToken token = recordTokens.get(tokenIndex);
        switch (token.kind) {
            case PROPERTY:
                if (current == null) {
                    return;
                }
                if (!(current instanceof JSONObject)) {
                    throw typeMismatch(recordPath, "object before property '" + token.property + "'", current);
                }
                traverse(((JSONObject) current).get(token.property), tokenIndex + 1,
                        wildcardIndex, bindings, state);
                return;
            case INDEX:
                if (current == null) {
                    return;
                }
                if (!(current instanceof JSONArray)) {
                    throw typeMismatch(recordPath, "array before index [" + token.index + "]", current);
                }
                JSONArray indexedArray = (JSONArray) current;
                if (indexedArray.size() <= token.index) {
                    return;
                }
                traverse(indexedArray.get(token.index), tokenIndex + 1,
                        wildcardIndex, bindings, state);
                return;
            case WILDCARD:
                if (current == null) {
                    if (wildcardIndex == 0) {
                        state.setSourceRowCount(0);
                    }
                    return;
                }
                if (!(current instanceof JSONArray)) {
                    throw typeMismatch(recordPath, "array before [*]", current);
                }
                JSONArray array = (JSONArray) current;
                if (wildcardIndex == 0) {
                    state.setSourceRowCount(array.size());
                }
                for (int i = 0; i < array.size() && !state.isAtLimit(); i++) {
                    bindings.put(wildcardIndex, i);
                    traverse(array.get(i), tokenIndex + 1, wildcardIndex + 1, bindings, state);
                }
                bindings.remove(wildcardIndex);
                return;
            default:
                throw new IllegalStateException("unknown path token");
        }
    }

    private Object resolveProjectionParent(JSON response,
                                           List<PathToken> tokens,
                                           String outputName,
                                           Map<Integer, Integer> bindings) {
        Object current = response;
        int wildcardIndex = 0;
        StringBuilder path = new StringBuilder();
        for (PathToken token : tokens) {
            if (token.kind == TokenKind.PROPERTY) {
                path.append(path.length() == 0 ? token.property : "." + token.property);
                if (current == null) {
                    return null;
                }
                if (!(current instanceof JSONObject)) {
                    throw projectionTypeMismatch(outputName, path.toString(), "object", current);
                }
                current = ((JSONObject) current).get(token.property);
            } else if (token.kind == TokenKind.INDEX) {
                path.append('[').append(token.index).append(']');
                if (current == null) {
                    return null;
                }
                if (!(current instanceof JSONArray)) {
                    throw projectionTypeMismatch(outputName, path.toString(), "array", current);
                }
                JSONArray array = (JSONArray) current;
                if (array.size() <= token.index) {
                    return null;
                }
                current = array.get(token.index);
            } else {
                Integer boundIndex = bindings.get(wildcardIndex);
                path.append("[*]");
                wildcardIndex++;
                if (boundIndex == null) {
                    throw new IllegalStateException("compiled wildcard binding is missing for " + outputName);
                }
                if (current == null) {
                    return null;
                }
                if (!(current instanceof JSONArray)) {
                    throw projectionTypeMismatch(outputName, path.toString(), "array", current);
                }
                JSONArray array = (JSONArray) current;
                if (array.size() <= boundIndex) {
                    return null;
                }
                current = array.get(boundIndex);
            }
        }
        if (current != null && !(current instanceof JSONObject)) {
            throw projectionTypeMismatch(outputName, path.toString(), "object", current);
        }
        return current;
    }

    private IllegalArgumentException typeMismatch(String path, String expected, Object actual) {
        return new IllegalArgumentException("JSON node type mismatch at " + path + ": expected "
                + expected + " but found " + actual.getClass().getSimpleName());
    }

    private IllegalArgumentException projectionTypeMismatch(String outputName,
                                                            String path,
                                                            String expected,
                                                            Object actual) {
        return new IllegalArgumentException("JSON projection type mismatch for " + outputName + " at "
                + (path.isEmpty() ? "$" : path) + ": expected " + expected + " but found "
                + actual.getClass().getSimpleName());
    }

    private static final class ExtractionState {
        private final JSON response;
        private final JSONArray rows;
        private final int maxRows;
        private int sourceRowCount;
        private boolean sourceRowCountSet;

        private ExtractionState(JSON response, JSONArray rows, int maxRows) {
            this.response = response;
            this.rows = rows;
            this.maxRows = maxRows;
        }

        private boolean isAtLimit() {
            return maxRows > 0 && rows.size() >= maxRows;
        }

        private void setSourceRowCount(int count) {
            sourceRowCount = count;
            sourceRowCountSet = true;
        }
    }
}
