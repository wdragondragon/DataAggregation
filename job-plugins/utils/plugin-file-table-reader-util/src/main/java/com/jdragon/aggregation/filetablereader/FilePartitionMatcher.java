package com.jdragon.aggregation.filetablereader;

import org.apache.commons.lang3.StringUtils;

import java.util.Locale;
import java.util.regex.Pattern;

final class FilePartitionMatcher {

    private final Pattern pattern;

    private FilePartitionMatcher(Pattern pattern) {
        this.pattern = pattern;
    }

    static FilePartitionMatcher compile(String partitionType, String partition) {
        String normalizedType = StringUtils.defaultIfBlank(partitionType, "glob")
                .trim()
                .toLowerCase(Locale.ENGLISH);
        String expression = StringUtils.defaultIfBlank(partition, "*");
        if ("regex".equals(normalizedType)) {
            return new FilePartitionMatcher(Pattern.compile(expression));
        }
        if ("glob".equals(normalizedType)) {
            return new FilePartitionMatcher(Pattern.compile(globToRegex(expression)));
        }
        throw new IllegalArgumentException("Unsupported partitionType: " + partitionType + ". Only glob and regex are supported.");
    }

    boolean matches(String relativePath) {
        return pattern.matcher(stripLeadingSlash(normalizePath(relativePath))).matches();
    }

    private static String globToRegex(String glob) {
        String normalized = StringUtils.defaultIfBlank(glob, "*").replace('\\', '/');
        StringBuilder regex = new StringBuilder("^");
        boolean inGroup = false;
        for (int i = 0; i < normalized.length(); i++) {
            char c = normalized.charAt(i);
            switch (c) {
                case '*':
                    if (i + 1 < normalized.length() && normalized.charAt(i + 1) == '*') {
                        regex.append(".*");
                        i++;
                    } else {
                        regex.append("[^/]*");
                    }
                    break;
                case '?':
                    regex.append("[^/]");
                    break;
                case '{':
                    regex.append('(');
                    inGroup = true;
                    break;
                case '}':
                    regex.append(')');
                    inGroup = false;
                    break;
                case ',':
                    regex.append(inGroup ? '|' : ',');
                    break;
                case '.':
                case '(':
                case ')':
                case '+':
                case '|':
                case '^':
                case '$':
                case '@':
                case '%':
                    regex.append('\\').append(c);
                    break;
                default:
                    regex.append(c);
                    break;
            }
        }
        return regex.append('$').toString();
    }

    private static String normalizePath(String value) {
        if (value == null) {
            return "";
        }
        String normalized = value.trim().replace('\\', '/');
        while (normalized.contains("//")) {
            normalized = normalized.replace("//", "/");
        }
        return normalized;
    }

    private static String stripLeadingSlash(String value) {
        String result = normalizePath(value);
        while (result.startsWith("/")) {
            result = result.substring(1);
        }
        return result;
    }
}
