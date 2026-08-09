package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.datasource.file.transfer.TransferFilePage;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.transfer.model.TransferSelection;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class FileDiscovery {

    private static final int PAGE_SIZE = 1_000;

    public int discover(TransferFileSystem source, TransferSelection selection,
                        Consumer<DiscoveredFile> consumer) throws IOException {
        TransferFileEntry configuredRoot = source.stat(selection.rootPath());
        String relativeRoot = configuredRoot.directory()
                ? configuredRoot.path()
                : TransferPaths.parent(configuredRoot.path());
        List<String> selectedPaths = selection.paths().isEmpty()
                ? List.of(selection.rootPath())
                : selection.paths();
        FileMatcher matcher = new FileMatcher(selection);
        int[] count = {0};
        for (String selectedPath : selectedPaths) {
            String resolvedPath = resolveSelectedPath(selection.rootPath(), selectedPath,
                    selection.paths().isEmpty());
            TransferFileEntry entry = source.stat(resolvedPath);
            if (entry.directory()) {
                walkDirectory(source, relativeRoot, entry.path(), selection.recursive(), matcher, consumer,
                        selection.maxFiles(), count);
            } else {
                accept(relativeRoot, entry, matcher, consumer, selection.maxFiles(), count);
            }
        }
        return count[0];
    }

    private String resolveSelectedPath(String rootPath, String selectedPath, boolean rootSelection) {
        if (rootSelection || selectedPath == null || selectedPath.isBlank()) {
            return rootPath;
        }
        String normalizedRoot = TransferPaths.normalize(rootPath);
        String normalizedSelected = TransferPaths.normalize(selectedPath);
        if (TransferPaths.isAbsolute(normalizedSelected)
                || normalizedSelected.equalsIgnoreCase(normalizedRoot)
                || normalizedSelected.regionMatches(true, 0, normalizedRoot + "/", 0,
                normalizedRoot.length() + 1)) {
            return normalizedSelected;
        }
        return TransferPaths.join(rootPath, selectedPath);
    }

    private void walkDirectory(TransferFileSystem source, String root, String start,
                               boolean recursive, FileMatcher matcher, Consumer<DiscoveredFile> consumer,
                               int maxFiles, int[] count) throws IOException {
        Deque<String> directories = new ArrayDeque<>();
        directories.add(start);
        while (!directories.isEmpty()) {
            String directory = directories.removeFirst();
            String cursor = null;
            do {
                TransferFilePage page = source.listPage(directory, cursor, PAGE_SIZE);
                for (TransferFileEntry child : page.entries()) {
                    if (child.directory()) {
                        if (recursive) {
                            directories.addLast(child.path());
                        }
                    } else {
                        accept(root, child, matcher, consumer, maxFiles, count);
                    }
                }
                String next = page.nextCursor();
                if (page.truncated() && (next == null || next.equals(cursor))) {
                    throw new IOException("file source returned a non-progressing page cursor for " + directory);
                }
                cursor = page.truncated() ? next : null;
            } while (cursor != null);
        }
    }

    private void accept(String root, TransferFileEntry entry, FileMatcher matcher,
                        Consumer<DiscoveredFile> consumer, int maxFiles, int[] count) throws IOException {
        String relativePath;
        try {
            relativePath = TransferPaths.relativeTo(root, entry.path());
        } catch (IllegalArgumentException e) {
            throw new IOException(e.getMessage(), e);
        }
        if (!matcher.matches(relativePath, entry)) {
            return;
        }
        if (count[0] >= maxFiles) {
            throw new IOException("file discovery limit exceeded: " + maxFiles);
        }
        consumer.accept(new DiscoveredFile(relativePath, entry));
        count[0]++;
    }

    private static final class FileMatcher {
        private final List<Pattern> includes;
        private final List<Pattern> excludes;
        private final Pattern regex;
        private final TransferSelection selection;

        private FileMatcher(TransferSelection selection) {
            this.selection = selection;
            this.includes = compileGlobs(selection.includeGlobs());
            this.excludes = compileGlobs(selection.excludeGlobs());
            this.regex = selection.includeRegex() == null || selection.includeRegex().isBlank()
                    ? null
                    : Pattern.compile(selection.includeRegex());
        }

        private boolean matches(String relativePath, TransferFileEntry entry) {
            String normalized = TransferPaths.normalize(relativePath);
            String name = entry.name();
            if (!includes.isEmpty() && includes.stream().noneMatch(pattern ->
                    pattern.matcher(normalized).matches() || pattern.matcher(name).matches())) {
                return false;
            }
            if (excludes.stream().anyMatch(pattern ->
                    pattern.matcher(normalized).matches() || pattern.matcher(name).matches())) {
                return false;
            }
            if (regex != null && !regex.matcher(normalized).find()) {
                return false;
            }
            return (selection.minSize() == null || entry.size() >= selection.minSize())
                    && (selection.maxSize() == null || entry.size() <= selection.maxSize())
                    && (selection.modifiedAfterMillis() == null
                    || entry.modifiedTimeMillis() >= selection.modifiedAfterMillis())
                    && (selection.modifiedBeforeMillis() == null
                    || entry.modifiedTimeMillis() <= selection.modifiedBeforeMillis());
        }

        private static List<Pattern> compileGlobs(List<String> globs) {
            List<Pattern> patterns = new ArrayList<>();
            for (String glob : globs) {
                if (glob != null && !glob.isBlank()) {
                    patterns.add(Pattern.compile(globRegex(TransferPaths.normalize(glob))));
                }
            }
            return patterns;
        }

        private static String globRegex(String glob) {
            StringBuilder regex = new StringBuilder("^");
            for (int index = 0; index < glob.length(); index++) {
                char value = glob.charAt(index);
                if (value == '*') {
                    if (index + 1 < glob.length() && glob.charAt(index + 1) == '*') {
                        if (index + 2 < glob.length() && glob.charAt(index + 2) == '/') {
                            regex.append("(?:.*/)?");
                            index += 2;
                        } else {
                            regex.append(".*");
                            index++;
                        }
                    } else {
                        regex.append("[^/]*");
                    }
                } else if (value == '?') {
                    regex.append("[^/]");
                } else {
                    if (".[]{}()+-^$|\\".indexOf(value) >= 0) {
                        regex.append('\\');
                    }
                    regex.append(value);
                }
            }
            return regex.append('$').toString();
        }
    }
}
