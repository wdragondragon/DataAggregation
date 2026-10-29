package com.jdragon.aggregation.commons.util.json;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class JsonRecordExtractor {
    private JsonRecordExtractor() {
    }

    /**
     * Compiles an explicit row path and absolute field parent paths. A wildcard in a field parent
     * must match the same complete prefix in {@code recordPath}; sibling array expansion is rejected.
     * Source names are literal object property names and are never interpreted as paths.
     */
    public static JsonExtractionPlan compile(String recordPath, List<JsonFieldProjection> projections) {
        if (recordPath == null || recordPath.trim().isEmpty()) {
            throw new IllegalArgumentException("recordPath must not be blank");
        }
        List<JsonFieldProjection> fields = projections == null
                ? Collections.emptyList()
                : new ArrayList<>(projections);
        List<PathToken> rowTokens = parsePath(recordPath.trim(), "recordPath");

        List<CompiledProjection> compiledProjections = new ArrayList<>(fields.size());
        Set<String> outputNames = new HashSet<>();
        for (int i = 0; i < fields.size(); i++) {
            JsonFieldProjection projection = fields.get(i);
            if (projection == null) {
                throw new IllegalArgumentException("projection at index " + i + " must not be null");
            }
            String outputName = trimToNull(projection.getName());
            if (outputName == null) {
                throw new IllegalArgumentException("projection name must not be blank at index " + i);
            }
            String sourceName = trimToNull(projection.getSourceName());
            if (sourceName == null) {
                sourceName = outputName;
            }
            String parentNode = trimToNull(projection.getParentNode());
            List<PathToken> parentTokens = parsePath(parentNode == null ? "$" : parentNode,
                    "parentNode for output " + outputName);
            validateWildcardBindings(parentTokens, rowTokens, outputName);

            if (!outputNames.add(outputName)) {
                CompiledProjection previous = findProjection(compiledProjections, outputName);
                if (previous == null || !previous.sourceName.equals(sourceName)
                        || !previous.parentTokens.equals(parentTokens)) {
                    throw new IllegalArgumentException("duplicate output name has conflicting projection: " + outputName);
                }
                continue;
            }
            compiledProjections.add(new CompiledProjection(outputName, sourceName, parentTokens));
        }

        return new JsonExtractionPlan(recordPath, rowTokens, compiledProjections);
    }

    private static CompiledProjection findProjection(List<CompiledProjection> projections, String outputName) {
        for (CompiledProjection projection : projections) {
            if (projection.outputName.equals(outputName)) {
                return projection;
            }
        }
        return null;
    }

    private static void validateWildcardBindings(List<PathToken> projectionTokens,
                                                List<PathToken> recordTokens,
                                                String outputName) {
        int recordWildcard = 0;
        for (int i = 0; i < projectionTokens.size(); i++) {
            PathToken token = projectionTokens.get(i);
            if (token.kind != TokenKind.WILDCARD) {
                continue;
            }
            while (recordWildcard < recordTokens.size()
                    && recordTokens.get(recordWildcard).kind != TokenKind.WILDCARD) {
                recordWildcard++;
            }
            if (recordWildcard >= recordTokens.size()) {
                throw new IllegalArgumentException("projection for " + outputName
                        + " contains an unbound wildcard");
            }
            if (!sameTokensThrough(projectionTokens, i, recordTokens, recordWildcard)) {
                throw new IllegalArgumentException("projection wildcard path for " + outputName
                        + " is not a prefix of recordPath");
            }
            recordWildcard++;
        }
    }

    private static boolean sameTokensThrough(List<PathToken> left, int leftEnd,
                                             List<PathToken> right, int rightEnd) {
        if (leftEnd != rightEnd) {
            return false;
        }
        for (int i = 0; i <= leftEnd; i++) {
            PathToken leftToken = left.get(i);
            PathToken rightToken = right.get(i);
            if (!leftToken.equals(rightToken)) {
                return false;
            }
        }
        return true;
    }

    private static List<PathToken> parsePath(String path, String label) {
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException(label + " must not be blank");
        }
        List<PathToken> tokens = new ArrayList<>();
        int index = 0;
        if (path.charAt(index) == '$') {
            index++;
            if (index == path.length()) {
                return Collections.emptyList();
            }
            if (path.charAt(index) == '.') {
                index++;
                if (index == path.length() || path.charAt(index) == '[') {
                    throw invalidPath(path, label);
                }
            } else if (path.charAt(index) != '[') {
                throw invalidPath(path, label);
            }
        }

        boolean expectProperty = index < path.length() && path.charAt(index) != '[';
        while (index < path.length()) {
            if (expectProperty) {
                int start = index;
                while (index < path.length() && path.charAt(index) != '.' && path.charAt(index) != '[') {
                    char character = path.charAt(index);
                    if (character == ']' || character == '$' || character == '*' || character == '?'
                            || character == '@' || character == '(' || character == ')' || character == '\''
                            || character == '"' || character == '\\' || character == ',' || character == ':'
                            || Character.isWhitespace(character)) {
                        throw invalidPath(path, label);
                    }
                    index++;
                }
                String property = path.substring(start, index);
                if (property.isEmpty()) {
                    throw invalidPath(path, label);
                }
                tokens.add(PathToken.property(property));
                expectProperty = false;
            }

            while (index < path.length() && path.charAt(index) == '[') {
                int close = path.indexOf(']', index + 1);
                if (close < 0) {
                    throw invalidPath(path, label);
                }
                String selector = path.substring(index + 1, close);
                if ("*".equals(selector)) {
                    tokens.add(PathToken.wildcard());
                } else if (selector.matches("\\d+")) {
                    try {
                        tokens.add(PathToken.index(Integer.parseInt(selector)));
                    } catch (NumberFormatException ex) {
                        throw new IllegalArgumentException("array index exceeds integer range in " + label + ": " + path, ex);
                    }
                } else {
                    throw invalidPath(path, label);
                }
                index = close + 1;
            }

            if (index == path.length()) {
                break;
            }
            if (path.charAt(index) != '.') {
                throw invalidPath(path, label);
            }
            index++;
            if (index == path.length() || path.charAt(index) == '[' || path.charAt(index) == '.') {
                throw invalidPath(path, label);
            }
            expectProperty = true;
        }
        return Collections.unmodifiableList(tokens);
    }

    private static IllegalArgumentException invalidPath(String path, String label) {
        return new IllegalArgumentException("unsupported " + label + " syntax: " + path);
    }

    private static String trimToNull(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return value.trim();
    }

    enum TokenKind {
        PROPERTY,
        WILDCARD,
        INDEX
    }

    static final class PathToken {
        final TokenKind kind;
        final String property;
        final int index;

        private PathToken(TokenKind kind, String property, int index) {
            this.kind = kind;
            this.property = property;
            this.index = index;
        }

        private static PathToken property(String property) {
            return new PathToken(TokenKind.PROPERTY, property, -1);
        }

        private static PathToken wildcard() {
            return new PathToken(TokenKind.WILDCARD, null, -1);
        }

        private static PathToken index(int index) {
            return new PathToken(TokenKind.INDEX, null, index);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof PathToken)) {
                return false;
            }
            PathToken that = (PathToken) other;
            return kind == that.kind && index == that.index && Objects.equals(property, that.property);
        }

        @Override
        public int hashCode() {
            return Objects.hash(kind, property, index);
        }

        @Override
        public String toString() {
            switch (kind) {
                case PROPERTY:
                    return property;
                case WILDCARD:
                    return "[*]";
                case INDEX:
                    return "[" + index + "]";
                default:
                    throw new IllegalStateException("unknown token kind");
            }
        }
    }

    static final class CompiledProjection {
        final String outputName;
        final String sourceName;
        final List<PathToken> parentTokens;

        private CompiledProjection(String outputName, String sourceName, List<PathToken> parentTokens) {
            this.outputName = outputName;
            this.sourceName = sourceName;
            this.parentTokens = parentTokens;
        }
    }
}
