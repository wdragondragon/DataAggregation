package com.jdragon.aggregation.pluginloader;

import com.jdragon.aggregation.pluginloader.type.IPluginType;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeSession;

public final class ClassLoaderSwapper{
    private ClassLoader storeClassLoader = null;
    private PluginRuntimeSession ownedRuntimeSession;

    private ClassLoaderSwapper() {
    }

    public static ClassLoaderSwapper newCurrentThreadClassLoaderSwapper() {
        return new ClassLoaderSwapper();
    }

    public static ClassLoaderSwapper newCurrentThreadClassLoaderSwapper(IPluginType pluginType, String pluginName) {
        ClassLoaderSwapper classLoaderSwapper = new ClassLoaderSwapper();
        classLoaderSwapper.setCurrentThreadClassLoader(pluginType, pluginName);
        return classLoaderSwapper;
    }

    public void setCurrentThreadClassLoader(IPluginType pluginType, String pluginName) {
        boolean openedRuntimeSession = false;
        if (PluginRuntimeSession.current() == null) {
            ownedRuntimeSession = PluginRuntimeSession.open();
            openedRuntimeSession = true;
        }
        try {
            JarLoader jarLoader = LoadUtil.getJarLoader(pluginType, pluginName);
            setCurrentThreadClassLoader(jarLoader);
        } catch (RuntimeException | Error ex) {
            if (openedRuntimeSession) {
                ownedRuntimeSession.close();
                ownedRuntimeSession = null;
            }
            throw ex;
        }
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
        if (ownedRuntimeSession != null) {
            ownedRuntimeSession.close();
            ownedRuntimeSession = null;
        }
        return classLoader;
    }
}
