package com.jdragon.aggregation.pluginloader.runtime;

import com.jdragon.aggregation.pluginloader.type.IPluginType;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Immutable plugin location resolved by the runtime integration.
 * The identity is opaque to the class loader and only separates revisions.
 */
public final class ResolvedPlugin {
    private final IPluginType pluginType;
    private final String pluginName;
    private final Path directory;
    private final String identity;

    public ResolvedPlugin(IPluginType pluginType, String pluginName, Path directory, String identity) {
        this.pluginType = Objects.requireNonNull(pluginType, "pluginType");
        this.pluginName = requireText(pluginName, "pluginName");
        this.directory = Objects.requireNonNull(directory, "directory").toAbsolutePath().normalize();
        this.identity = requireText(identity, "identity");
    }

    public IPluginType getPluginType() {
        return pluginType;
    }

    public String getPluginName() {
        return pluginName;
    }

    public Path getDirectory() {
        return directory;
    }

    public String getIdentity() {
        return identity;
    }

    public String getCoordinate() {
        return pluginType.getName() + "/" + pluginName;
    }

    public String getRuntimeKey() {
        return getCoordinate() + "@" + identity;
    }

    private static String requireText(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value.trim();
    }
}
