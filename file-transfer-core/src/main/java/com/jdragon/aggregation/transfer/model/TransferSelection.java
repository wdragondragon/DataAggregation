package com.jdragon.aggregation.transfer.model;

import java.util.List;

public record TransferSelection(String rootPath,
                                List<String> paths,
                                boolean recursive,
                                List<String> includeGlobs,
                                String includeRegex,
                                List<String> excludeGlobs,
                                Long minSize,
                                Long maxSize,
                                Long modifiedAfterMillis,
                                Long modifiedBeforeMillis,
                                int maxFiles) {

    public TransferSelection {
        rootPath = rootPath == null || rootPath.isBlank() ? "." : rootPath;
        paths = paths == null ? List.of() : List.copyOf(paths);
        includeGlobs = includeGlobs == null ? List.of() : List.copyOf(includeGlobs);
        excludeGlobs = excludeGlobs == null ? List.of() : List.copyOf(excludeGlobs);
        if (minSize != null && minSize < 0) {
            throw new IllegalArgumentException("selection minSize must be non-negative");
        }
        if (maxSize != null && maxSize < 0) {
            throw new IllegalArgumentException("selection maxSize must be non-negative");
        }
        if (minSize != null && maxSize != null && minSize > maxSize) {
            throw new IllegalArgumentException("selection minSize must not exceed maxSize");
        }
        if (modifiedAfterMillis != null && modifiedBeforeMillis != null
                && modifiedAfterMillis > modifiedBeforeMillis) {
            throw new IllegalArgumentException("selection modifiedAfterMillis must not exceed modifiedBeforeMillis");
        }
        if (maxFiles <= 0) {
            maxFiles = 100_000;
        }
    }
}
