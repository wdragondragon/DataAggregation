package com.jdragon.aggregation.pluginloader.spi;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.pluginloader.type.IPluginType;

public abstract class AbstractPlugin implements PluginAble {

    private IPluginType pluginType;

    //插件本身的plugin
    private Configuration pluginConf;

    @Override
    public void setPluginType(IPluginType pluginType) {
        this.pluginType = pluginType;
    }

    @Override
    public IPluginType getPluginType() {
        return pluginType;
    }

    @Override
    public String getPluginName() {
        assert null != this.pluginConf;
        return this.pluginConf.getString("name");
    }

    @Override
    public String getDeveloper() {
        assert null != this.pluginConf;
        return this.pluginConf.getString("developer");
    }

    @Override
    public String getDescription() {
        assert null != this.pluginConf;
        return this.pluginConf.getString("description");
    }

    @Override
    public void setPluginConf(Configuration pluginConf) {
        this.pluginConf = pluginConf;
    }

    @Override
    public void init() {
        
    }

    @Override
    public void destroy() {

    }


}
