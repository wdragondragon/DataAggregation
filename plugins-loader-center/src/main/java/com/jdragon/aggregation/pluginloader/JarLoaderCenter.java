package com.jdragon.aggregation.pluginloader;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
public class JarLoaderCenter {
    /**
     * jarLoader的缓冲
     */
    private static final Map<String, JarLoader> jarLoaderCenter = new HashMap<>();

    public static synchronized JarLoader getJarLoader(String key, String pluginPath) {
        JarLoader jarLoader = jarLoaderCenter.get(key);
        if (null == jarLoader) {
            jarLoader = new JarLoader(new String[]{pluginPath});
            jarLoaderCenter.put(key, jarLoader);
        }

        return jarLoader;
    }

    public static synchronized void clearJarLoader() {
        Set<String> keySet = new HashSet<>(jarLoaderCenter.keySet());
        for (String key : keySet) {
            removeJarLoader(key);
        }
    }

    public static synchronized void removeJarLoader(String key) {
        JarLoader jarLoader = jarLoaderCenter.get(key);
        if (null != jarLoader) {
            jarLoader.clearAssertionStatus();
            try {
                jarLoader.close();
            } catch (IOException e) {
                log.error("jar loader close error:{}", e.getMessage(), e);
            }
            JarLoader remove = jarLoaderCenter.remove(key);
            if (null != remove) {
                log.info("remove jar loader:{}", key);
            }
        }
    }
}