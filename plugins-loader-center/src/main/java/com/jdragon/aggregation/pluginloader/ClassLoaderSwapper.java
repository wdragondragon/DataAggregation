package com.jdragon.aggregation.pluginloader;

import com.jdragon.aggregation.pluginloader.type.IPluginType;

public final class ClassLoaderSwapper {
    private ClassLoader storeClassLoader = null;

    private ClassLoaderSwapper() {
    }

    public static ClassLoaderSwapper newCurrentThreadClassLoaderSwapper() {
        return new ClassLoaderSwapper();
    }

    public void setCurrentThreadClassLoader(IPluginType pluginType, String pluginName) {
        JarLoader jarLoader = LoadUtil.getJarLoader(pluginType, pluginName);
        setCurrentThreadClassLoader(jarLoader);
    }

    /**
     * 保存当前classLoader，并将当前线程的classLoader设置为所给classLoader
     */
    public ClassLoader setCurrentThreadClassLoader(ClassLoader classLoader) {
        this.storeClassLoader = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(classLoader);
        return this.storeClassLoader;
    }

    /**
     * 将当前线程的类加载器设置为保存的类加载
     */
    public ClassLoader restoreCurrentThreadClassLoader() {
        ClassLoader classLoader = Thread.currentThread()
                .getContextClassLoader();
        Thread.currentThread().setContextClassLoader(this.storeClassLoader);
        return classLoader;
    }
}