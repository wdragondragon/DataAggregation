package com.jdragon.aggregation.pluginloader.runtime;

import com.jdragon.aggregation.pluginloader.type.IPluginType;

import java.util.Objects;
import java.util.function.Function;

/** Resolves a logical plugin name to a locally readable immutable directory. */
public interface PluginRuntimeResolver {
    ResolvedPlugin resolve(IPluginType pluginType, String pluginName);

    /**
     * Resolves a plugin and performs the caller's lease operation while the resolver can
     * protect the returned directory from concurrent cache cleanup.  Local resolvers do
     * not need a critical section, while materializing resolvers override this method to
     * hold their active-release lock across the callback.
     */
    default <T> T withResolvedPlugin(IPluginType pluginType, String pluginName,
                                     Function<ResolvedPlugin, T> action) {
        return Objects.requireNonNull(action, "action").apply(resolve(pluginType, pluginName));
    }
}
