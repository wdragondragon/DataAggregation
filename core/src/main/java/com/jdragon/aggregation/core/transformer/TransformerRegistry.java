package com.jdragon.aggregation.core.transformer;

import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.core.transformer.internal.*;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import com.jdragon.aggregation.pluginloader.LoadUtil;
import com.jdragon.aggregation.pluginloader.constant.SystemConstants;
import com.jdragon.aggregation.pluginloader.runtime.ResolvedPlugin;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeSession;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;

public class TransformerRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(TransformerRegistry.class);

    public static String TRANSFORMER_HOME = StringUtils.join(
            new String[]{SystemConstants.PLUGIN_HOME, PluginType.TRANSFORMER.getName()}, File.separator);

    private static final Map<String, TransformerInfo> nativeTransformers = new ConcurrentHashMap<String, TransformerInfo>();
    private static final Map<String, WeakReference<TransformerInfo>> externalTransformers =
            new ConcurrentHashMap<String, WeakReference<TransformerInfo>>();

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
        if (transformers != null) {
            for (String transformer : transformers) {
                if (transformer != null && !transformer.startsWith("dx_")) {
                    getTransformer(transformer);
                }
            }
            return;
        }
        TRANSFORMER_HOME = StringUtils.join(
                new String[]{SystemConstants.PLUGIN_HOME, PluginType.TRANSFORMER.getName()}, File.separator);
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
        loadTransformerInfo(each);
    }

    private static TransformerInfo loadTransformerInfo(String each) {
        String transformerPath = TRANSFORMER_HOME + File.separator + each;
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.TRANSFORMER, each)) {
            ResolvedPlugin resolvedPlugin = LoadUtil.resolvePlugin(PluginType.TRANSFORMER, each);
            transformerPath = resolvedPlugin.getDirectory().toString();
            String runtimeKey = transformerRuntimeKey(each, resolvedPlugin);
            PluginRuntimeSession session = PluginRuntimeSession.current();
            if (session != null) {
                Object existing = session.getRuntimeObject("transformer:" + runtimeKey);
                if (existing instanceof TransformerInfo) {
                    return (TransformerInfo) existing;
                }
            } else {
                WeakReference<TransformerInfo> reference = externalTransformers.get(runtimeKey);
                TransformerInfo existing = reference == null ? null : reference.get();
                if (existing != null) {
                    return existing;
                }
            }
            Transformer transformer = classLoaderSwapper.loadPlugin();
            transformer.setTransformerName(each);
            TransformerInfo info = buildTransformerInfo(transformer, false, transformer.getClassLoader());
            externalTransformers.put(runtimeKey, new WeakReference<TransformerInfo>(info));
            if (session != null) {
                session.putRuntimeObject("transformer:" + runtimeKey, info);
            }
            return info;
        } catch (Exception e) {
            //错误function跳过
            LOG.error(String.format("skip transformer(%s),load Transformer class error, path = %s ", each, transformerPath), e);
            return null;
        }
    }

    public static TransformerInfo getTransformer(String transformerName) {
        if (transformerName == null) {
            return null;
        }
        TransformerInfo nativeTransformer = nativeTransformers.get(transformerName);
        if (nativeTransformer != null || transformerName.startsWith("dx_")) {
            return nativeTransformer;
        }
        ResolvedPlugin resolvedPlugin = LoadUtil.resolvePlugin(PluginType.TRANSFORMER, transformerName);
        String runtimeKey = transformerRuntimeKey(transformerName, resolvedPlugin);
        PluginRuntimeSession session = PluginRuntimeSession.current();
        if (session != null) {
            Object taskTransformer = session.getRuntimeObject("transformer:" + runtimeKey);
            if (taskTransformer instanceof TransformerInfo) {
                return (TransformerInfo) taskTransformer;
            }
            return loadTransformerInfo(transformerName);
        }
        WeakReference<TransformerInfo> reference = externalTransformers.get(runtimeKey);
        TransformerInfo transformerInfo = reference == null ? null : reference.get();
        return transformerInfo == null ? loadTransformerInfo(transformerName) : transformerInfo;
    }

    public static synchronized void registTransformer(Transformer transformer) {
        registTransformer(transformer, null, true);
    }

    public static synchronized void registTransformer(Transformer transformer, ClassLoader classLoader, boolean isNative) {
        registTransformer(transformer, classLoader, isNative, transformer.getTransformerName());
    }

    private static synchronized void registTransformer(Transformer transformer, ClassLoader classLoader,
                                                        boolean isNative, String runtimeKey) {

        checkName(transformer.getTransformerName(), isNative);

        if (isNative && nativeTransformers.containsKey(transformer.getTransformerName())) {
//            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_DUPLICATE_ERROR, " name=" + transformer.getTransformerName());
            LOG.warn("Transformer {} has existed.", transformer.getTransformerName());
            return;
        }

        TransformerInfo info = buildTransformerInfo(transformer, isNative, classLoader);
        if (isNative) {
            nativeTransformers.put(transformer.getTransformerName(), info);
        } else {
            externalTransformers.put(runtimeKey, new WeakReference<TransformerInfo>(info));
        }

    }

    private static String transformerRuntimeKey(String transformerName, ResolvedPlugin resolvedPlugin) {
        return transformerName + "@" + resolvedPlugin.getIdentity();
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
