package com.jdragon.aggregation.pluginloader.runtime;

import com.jdragon.aggregation.pluginloader.type.IPluginType;

import java.util.Objects;
import java.util.function.Function;

/** Process-wide resolver registry. Studio Worker installs an OSS-aware resolver at bootstrap. */
public final class PluginRuntimeResolvers {
    private static final PluginRuntimeResolver LOCAL = new LocalPluginRuntimeResolver();
    private static volatile PluginRuntimeResolver resolver = LOCAL;

    private PluginRuntimeResolvers() {
    }

    public static ResolvedPlugin resolve(IPluginType pluginType, String pluginName) {
        return resolver.resolve(pluginType, pluginName);
    }

    /**
     * Resolves and consumes a plugin through the installed resolver's protected operation.
     * Keeping this indirection here prevents the loader center from knowing how a resolver
     * materializes or retires local plugin revisions.
     */
    public static <T> T withResolvedPlugin(IPluginType pluginType, String pluginName,
                                           Function<ResolvedPlugin, T> action) {
        return resolver.withResolvedPlugin(pluginType, pluginName, action);
    }

    public static void install(PluginRuntimeResolver runtimeResolver) {
        resolver = Objects.requireNonNull(runtimeResolver, "runtimeResolver");
    }

    public static void reset() {
        resolver = LOCAL;
    }

    public static boolean isLocalResolver() {
        return resolver == LOCAL;
    }
}
