package com.jdragon.aggregation.pluginloader;

import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.pluginloader.constant.JarLoaderErrorCode;
import com.jdragon.aggregation.pluginloader.constant.SystemConstants;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import com.jdragon.aggregation.pluginloader.type.IPluginType;
import org.apache.commons.lang.StringUtils;

import java.io.File;

public class LoadUtil {

    private static final String pluginTypeNameFormat = "plugin.%s.%s";

    @SuppressWarnings("unchecked")
    public static <T extends AbstractPlugin> T loadJobPlugin(IPluginType pluginType,
                                                             String pluginName) {
        Class<? extends AbstractPlugin> clazz = LoadUtil.loadPluginClass(pluginType, pluginName);

        try {
            AbstractPlugin jobPlugin = clazz.newInstance();
            jobPlugin.setPluginConf(ConfigParser.parseOnePluginConfig(pluginType, pluginName));
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
            IPluginType pluginType, String pluginName) {
        JarLoader jarLoader = LoadUtil.getJarLoader(pluginType, pluginName);
        Configuration pluginConf = ConfigParser.parseOnePluginConfig(pluginType, pluginName);
        pluginConf = pluginConf.getConfiguration(generatePluginKey(pluginType, pluginName));
        try {
            return (Class<? extends AbstractPlugin>) jarLoader
                    .loadClass(pluginConf.getString("class"));
        } catch (Exception e) {
            throw AggregationException.asException(JarLoaderErrorCode.LOAD_PLUGIN_CLASS_ERROR, e);
        }
    }

    public static JarLoader getJarLoader(IPluginType pluginType, String pluginName) {
        String pluginKey = generatePluginKey(pluginType, pluginName);
        String pluginPath = StringUtils.join(
                new String[]{SystemConstants.PLUGIN_HOME, pluginType.getName(), pluginName}, File.separator);
        return JarLoaderCenter.getJarLoader(pluginKey, pluginPath);
    }

    private static String generatePluginKey(IPluginType pluginType,
                                            String pluginName) {
        return String.format(pluginTypeNameFormat, pluginType.getName(),
                pluginName);
    }

    public static void updateJarLoader(IPluginType pluginType, String pluginName) {
        String pluginKey = generatePluginKey(pluginType, pluginName);
        JarLoaderCenter.removeJarLoader(pluginKey);
    }

    public static void updateJarLoader() {
        JarLoaderCenter.clearJarLoader();
    }
}
