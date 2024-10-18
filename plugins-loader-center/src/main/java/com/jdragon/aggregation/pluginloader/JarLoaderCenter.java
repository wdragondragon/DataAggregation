package com.jdragon.aggregation.pluginloader;

import java.util.HashMap;
import java.util.Map;

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
}
