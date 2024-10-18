package com.jdragon.aggregation.pluginloader.spi;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.pluginloader.type.IPluginType;

public interface PluginAble {
    String getDeveloper();

    String getDescription();

    void setPluginConf(Configuration pluginConf);

    void init();

    void destroy();

    String getPluginName();

    void setPluginType(IPluginType pluginType);

    IPluginType getPluginType();
}
