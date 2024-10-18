package com.jdragon.aggregation.core.plugin;

import com.jdragon.aggregation.pluginloader.type.IPluginType;

public enum PluginType implements IPluginType {
    READER("reader"),
    TRANSFORMER("transformer"),
    WRITER("writer"),
    ;

    private final String name;

    PluginType(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }
}
