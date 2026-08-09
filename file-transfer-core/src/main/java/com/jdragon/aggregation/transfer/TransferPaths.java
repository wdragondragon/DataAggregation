package com.jdragon.aggregation.transfer;

import java.util.regex.Pattern;

public final class TransferPaths {

    private static final Pattern WINDOWS_ABSOLUTE = Pattern.compile("^[A-Za-z]:/.*");

    private TransferPaths() {
    }

    public static String normalize(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        String normalized = value.trim().replace('\\', '/');
        while (normalized.contains("//")) {
            normalized = normalized.replace("//", "/");
        }
        while (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    public static boolean isAbsolute(String value) {
        String normalized = normalize(value);
        return normalized.startsWith("/") || WINDOWS_ABSOLUTE.matcher(normalized).matches();
    }

    public static String join(String parent, String child) {
        String normalizedChild = normalize(child);
        if (normalizedChild.isEmpty()) {
            return normalize(parent);
        }
        if (isAbsolute(normalizedChild)) {
            return normalizedChild;
        }
        String normalizedParent = normalize(parent);
        if (normalizedParent.isEmpty() || ".".equals(normalizedParent)) {
            return normalizedChild;
        }
        return normalizedParent + "/" + normalizedChild;
    }

    public static String parent(String value) {
        String normalized = normalize(value);
        int slash = normalized.lastIndexOf('/');
        if (slash < 0) {
            return ".";
        }
        if (slash == 0) {
            return "/";
        }
        return normalized.substring(0, slash);
    }

    public static String fileName(String value) {
        String normalized = normalize(value);
        int slash = normalized.lastIndexOf('/');
        return slash < 0 ? normalized : normalized.substring(slash + 1);
    }

    public static String relativeTo(String root, String path) {
        String normalizedRoot = normalize(root);
        String normalizedPath = normalize(path);
        if (normalizedRoot.equalsIgnoreCase(normalizedPath)) {
            return fileName(normalizedPath);
        }
        String prefix = normalizedRoot.endsWith("/") ? normalizedRoot : normalizedRoot + "/";
        if (!normalizedPath.regionMatches(true, 0, prefix, 0, prefix.length())) {
            throw new IllegalArgumentException("path is outside selection root: " + path);
        }
        return normalizedPath.substring(prefix.length());
    }
}
