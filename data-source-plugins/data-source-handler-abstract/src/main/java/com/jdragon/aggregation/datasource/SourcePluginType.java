package com.jdragon.aggregation.datasource;

import com.jdragon.aggregation.pluginloader.type.IPluginType;
import lombok.Getter;

@Getter
public enum SourcePluginType implements IPluginType {
    SOURCE("source"),
    ;

    private final String name;

    SourcePluginType(final String name) {
        this.name = name;
    }
}
