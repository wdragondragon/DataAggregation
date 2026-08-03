package com.jdragon.aggregation.pluginloader;

import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.pluginloader.constant.JarLoaderErrorCode;
import com.jdragon.aggregation.pluginloader.constant.SystemConstants;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeResolvers;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeSession;
import com.jdragon.aggregation.pluginloader.runtime.ResolvedPlugin;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import com.jdragon.aggregation.pluginloader.type.IPluginType;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.lang.ref.Cleaner;

public class LoadUtil {

    private static final String pluginTypeNameFormat = "plugin.%s.%s";
    private static final Cleaner pluginCleaner = Cleaner.create();

    @SuppressWarnings("unchecked")
    public static <T extends AbstractPlugin> T loadJobPlugin(IPluginType pluginType,
                                                              String pluginName) {
        if (PluginRuntimeSession.current() != null) {
            return loadJobPluginInCurrentSession(pluginType, pluginName);
        }
        PluginRuntimeSession ownedSession = PluginRuntimeSession.createDetached();
        try {
            T plugin = ownedSession.call(() -> loadJobPluginInCurrentSession(pluginType, pluginName));
            pluginCleaner.register(plugin, ownedSession::close);
            return plugin;
        } catch (RuntimeException | Error ex) {
            ownedSession.close();
            throw ex;
        }
    }

    @SuppressWarnings("unchecked")
    private static <T extends AbstractPlugin> T loadJobPluginInCurrentSession(IPluginType pluginType,
                                                                               String pluginName) {
        ResolvedPlugin resolvedPlugin = resolvePlugin(pluginType, pluginName);
        Class<? extends AbstractPlugin> clazz = LoadUtil.loadPluginClass(pluginType, pluginName, resolvedPlugin);

        try {
            AbstractPlugin jobPlugin = clazz.newInstance();
            jobPlugin.setPluginConf(ConfigParser.parseOnePluginConfig(resolvedPlugin, pluginType));
            jobPlugin.setPluginType(pluginType);
            jobPlugin.setClassLoader(clazz.getClassLoader());
            return (T) jobPlugin;
        } catch (InstantiationException | IllegalAccessException e) {
            throw AggregationException.asException(
                    JarLoaderErrorCode.CREATE_PLUGIN_ERROR,
                    String.format("找到plugin[%s]的Job配置.",
                            pluginName), e);
        } catch (ClassCastException e) {
            throw AggregationException.asException(
                    JarLoaderErrorCode.PLUGIN_CAST_ERROR,
                    String.format("找到plugin[%s]的Job配置.",
                            pluginName), e);
        } catch (Exception e) {
            throw AggregationException.asException(
                    JarLoaderErrorCode.PLUGIN_INIT_ERROR,
                    String.format("找到plugin[%s]的Job配置.",
                            pluginName), e);
        }
    }

    @SuppressWarnings("unchecked")
    private static synchronized Class<? extends AbstractPlugin> loadPluginClass(
            IPluginType pluginType, String pluginName, ResolvedPlugin resolvedPlugin) {
        JarLoader jarLoader = LoadUtil.getJarLoader(resolvedPlugin);
        Configuration pluginConf = ConfigParser.parseOnePluginConfig(resolvedPlugin, pluginType);
        pluginConf = pluginConf.getConfiguration(generatePluginKey(pluginType, pluginName));
        try {
            return (Class<? extends AbstractPlugin>) jarLoader
                    .loadClass(pluginConf.getString("class"));
        } catch (Exception e) {
            throw AggregationException.asException(JarLoaderErrorCode.LOAD_PLUGIN_CLASS_ERROR, e);
        }
    }

    public static JarLoader getJarLoader(IPluginType pluginType, String pluginName) {
        return getJarLoader(resolvePlugin(pluginType, pluginName));
    }

    public static ResolvedPlugin resolvePlugin(IPluginType pluginType, String pluginName) {
        PluginRuntimeSession session = PluginRuntimeSession.current();
        return session == null
                ? PluginRuntimeResolvers.resolve(pluginType, pluginName)
                : session.resolve(pluginType, pluginName);
    }

    private static JarLoader getJarLoader(ResolvedPlugin resolvedPlugin) {
        PluginRuntimeSession session = PluginRuntimeSession.current();
        return session == null
                ? JarLoaderCenter.getJarLoader(resolvedPlugin)
                : session.acquire(resolvedPlugin);
    }

    private static String generatePluginKey(IPluginType pluginType,
                                            String pluginName) {
        return String.format(pluginTypeNameFormat, pluginType.getName(),
                pluginName);
    }

    public static void updateJarLoader(IPluginType pluginType, String pluginName) {
        String pluginKey = generatePluginKey(pluginType, pluginName);
        JarLoaderCenter.removeJarLoader(pluginKey);
        ConfigParser.clearCache();
    }

    public static void updateJarLoader() {
        JarLoaderCenter.clearJarLoader();
        ConfigParser.clearCache();
    }
}
