package com.jdragon.aggregation.core.transformer;

import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.core.transformer.internal.*;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import com.jdragon.aggregation.pluginloader.constant.SystemConstants;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransformerRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(TransformerRegistry.class);

    public static String TRANSFORMER_HOME = StringUtils.join(
            new String[]{SystemConstants.PLUGIN_HOME, PluginType.TRANSFORMER.getName()}, File.separator);

    private static final Map<String, TransformerInfo> registedTransformer = new HashMap<String, TransformerInfo>();

    static {
        /**
         * add native transformer
         * local storage and from server will be delay load.
         */

        registTransformer(new SubstrTransformer());
        registTransformer(new PadTransformer());
        registTransformer(new ReplaceTransformer());
        registTransformer(new FilterTransformer());
        registTransformer(new GroovyTransformer());
    }

    public static void loadTransformerFromLocalStorage(List<String> transformers) {
        String[] paths = new File(TRANSFORMER_HOME).list();
        if (null == paths) {
            return;
        }

        for (final String each : paths) {
            try {
                if (transformers == null || transformers.contains(each)) {
                    loadTransformer(each);
                }
            } catch (Exception e) {
                LOG.error(String.format("skip transformer(%s) loadTransformer has Exception(%s)", each, e.getMessage()), e);
            }
        }
    }

    public static void loadTransformer(String each) {
        String transformerPath = TRANSFORMER_HOME + File.separator + each;
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.TRANSFORMER, each)) {
            Transformer transformer = classLoaderSwapper.loadPlugin();
            transformer.setTransformerName(each);
            registTransformer(transformer, transformer.getClassLoader(), false);
        } catch (Exception e) {
            //错误function跳过
            LOG.error(String.format("skip transformer(%s),load Transformer class error, path = %s ", each, transformerPath), e);
        }
    }

    public static TransformerInfo getTransformer(String transformerName) {
        return registedTransformer.get(transformerName);
    }

    public static synchronized void registTransformer(Transformer transformer) {
        registTransformer(transformer, null, true);
    }

    public static synchronized void registTransformer(Transformer transformer, ClassLoader classLoader, boolean isNative) {

        checkName(transformer.getTransformerName(), isNative);

        if (registedTransformer.containsKey(transformer.getTransformerName())) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_DUPLICATE_ERROR, " name=" + transformer.getTransformerName());
        }

        registedTransformer.put(transformer.getTransformerName(), buildTransformerInfo(transformer, isNative, classLoader));

    }

    private static void checkName(String functionName, boolean isNative) {
        boolean checkResult = true;
        if (isNative) {
            if (!functionName.startsWith("dx_")) {
                checkResult = false;
            }
        } else {
            if (functionName.startsWith("dx_")) {
                checkResult = false;
            }
        }

        if (!checkResult) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_NAME_ERROR, " name=" + functionName + ": isNative=" + isNative);
        }

    }

    private static TransformerInfo buildTransformerInfo(Transformer complexTransformer, boolean isNative, ClassLoader classLoader) {
        TransformerInfo transformerInfo = new TransformerInfo();
        transformerInfo.setClassLoader(classLoader);
        transformerInfo.setNative(isNative);
        transformerInfo.setTransformer(complexTransformer);
        return transformerInfo;
    }
}
