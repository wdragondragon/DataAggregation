package com.jdragon.aggregation.pluginloader;

import lombok.extern.slf4j.Slf4j;

import com.jdragon.aggregation.pluginloader.runtime.ResolvedPlugin;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class JarLoaderCenter {
    /**
     * jarLoader的缓冲
     */
    private static final Map<String, LoaderHolder> jarLoaderCenter = new HashMap<>();
    private static final Map<String, String> activeLoaderKeys = new HashMap<>();
    private static final Map<String, Long> generations = new HashMap<>();

    public static synchronized JarLoader getJarLoader(String key, String pluginPath) {
        LoaderHolder holder = jarLoaderCenter.get(key);
        if (holder == null) {
            holder = new LoaderHolder(key, key, normalizeDirectory(pluginPath),
                    new JarLoader(new String[]{pluginPath}));
            jarLoaderCenter.put(key, holder);
        }
        return holder.jarLoader;
    }

    public static synchronized LoaderLease acquire(ResolvedPlugin plugin) {
        String coordinate = plugin.getCoordinate();
        long generation = generations.getOrDefault(coordinate, 0L);
        String loaderKey = plugin.getRuntimeKey() + "#" + generation;
        String previousKey = activeLoaderKeys.put(coordinate, loaderKey);
        if (previousKey != null && !previousKey.equals(loaderKey)) {
            retire(previousKey);
        }
        LoaderHolder holder = jarLoaderCenter.get(loaderKey);
        if (holder == null) {
            holder = new LoaderHolder(loaderKey, coordinate, plugin.getDirectory(),
                    new JarLoader(new String[]{plugin.getDirectory().toString()}));
            jarLoaderCenter.put(loaderKey, holder);
        }
        holder.retired = false;
        holder.references++;
        return new LoaderLease(loaderKey, holder.jarLoader);
    }

    public static synchronized JarLoader getJarLoader(ResolvedPlugin plugin) {
        String coordinate = plugin.getCoordinate();
        long generation = generations.getOrDefault(coordinate, 0L);
        String loaderKey = plugin.getRuntimeKey() + "#" + generation;
        String previousKey = activeLoaderKeys.put(coordinate, loaderKey);
        if (previousKey != null && !previousKey.equals(loaderKey)) {
            retire(previousKey);
        }
        LoaderHolder holder = jarLoaderCenter.get(loaderKey);
        if (holder == null) {
            holder = new LoaderHolder(loaderKey, coordinate, plugin.getDirectory(),
                    new JarLoader(new String[]{plugin.getDirectory().toString()}));
            jarLoaderCenter.put(loaderKey, holder);
        }
        holder.retired = false;
        return holder.jarLoader;
    }

    public static synchronized void clearJarLoader() {
        Set<String> keySet = new HashSet<>(jarLoaderCenter.keySet());
        for (String key : keySet) {
            retire(key);
        }
        for (String coordinate : new HashSet<String>(activeLoaderKeys.keySet())) {
            generations.put(coordinate, generations.getOrDefault(coordinate, 0L) + 1L);
        }
        activeLoaderKeys.clear();
    }

    public static synchronized void removeJarLoader(String key) {
        String coordinate = normalizeLegacyCoordinate(key);
        generations.put(coordinate, generations.getOrDefault(coordinate, 0L) + 1L);
        String activeKey = activeLoaderKeys.remove(coordinate);
        if (activeKey != null) {
            retire(activeKey);
        }
        LoaderHolder legacy = jarLoaderCenter.get(key);
        if (legacy != null) {
            retire(key);
        }
    }

    public static synchronized boolean isInUse(String coordinate, String identity) {
        String prefix = coordinate + "@" + identity + "#";
        for (Map.Entry<String, LoaderHolder> entry : jarLoaderCenter.entrySet()) {
            if (entry.getKey().startsWith(prefix) && entry.getValue().references > 0) {
                return true;
            }
        }
        return false;
    }

    /** Returns whether a loader lease still references the materialized plugin directory. */
    public static synchronized boolean isDirectoryInUse(Path directory) {
        if (directory == null) {
            return false;
        }
        Path normalized = directory.toAbsolutePath().normalize();
        for (LoaderHolder holder : jarLoaderCenter.values()) {
            if (holder.references > 0 && normalized.equals(holder.pluginDirectory)) {
                return true;
            }
        }
        return false;
    }

    private static Path normalizeDirectory(String pluginPath) {
        return Path.of(pluginPath).toAbsolutePath().normalize();
    }

    private static String normalizeLegacyCoordinate(String key) {
        if (key != null && key.startsWith("plugin.")) {
            String value = key.substring("plugin.".length());
            int split = value.indexOf('.');
            if (split > 0 && split < value.length() - 1) {
                return value.substring(0, split) + "/" + value.substring(split + 1);
            }
        }
        return key;
    }

    private static void release(String loaderKey) {
        synchronized (JarLoaderCenter.class) {
            LoaderHolder holder = jarLoaderCenter.get(loaderKey);
            if (holder == null) {
                return;
            }
            holder.references = Math.max(0, holder.references - 1);
            closeIfRetired(holder);
        }
    }

    private static void retire(String loaderKey) {
        LoaderHolder holder = jarLoaderCenter.get(loaderKey);
        if (holder == null) {
            return;
        }
        holder.retired = true;
        closeIfRetired(holder);
    }

    private static void closeIfRetired(LoaderHolder holder) {
        if (!holder.retired || holder.references > 0) {
            return;
        }
        holder.jarLoader.clearAssertionStatus();
        try {
            holder.jarLoader.close();
        } catch (IOException e) {
            log.error("jar loader close error:{}", e.getMessage(), e);
        }
        if (jarLoaderCenter.remove(holder.loaderKey) != null) {
            log.info("remove jar loader:{}", holder.loaderKey);
        }
    }

    private static final class LoaderHolder {
        private final String loaderKey;
        private final String coordinate;
        private final Path pluginDirectory;
        private final JarLoader jarLoader;
        private int references;
        private boolean retired;

        private LoaderHolder(String loaderKey, String coordinate, Path pluginDirectory, JarLoader jarLoader) {
            this.loaderKey = loaderKey;
            this.coordinate = coordinate;
            this.pluginDirectory = pluginDirectory.toAbsolutePath().normalize();
            this.jarLoader = jarLoader;
        }
    }

    public static final class LoaderLease implements AutoCloseable {
        private final String loaderKey;
        private final JarLoader jarLoader;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        private LoaderLease(String loaderKey, JarLoader jarLoader) {
            this.loaderKey = loaderKey;
            this.jarLoader = jarLoader;
        }

        public JarLoader getJarLoader() {
            return jarLoader;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                release(loaderKey);
            }
        }
    }
}
