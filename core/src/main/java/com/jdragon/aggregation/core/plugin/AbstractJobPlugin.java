package com.jdragon.aggregation.core.plugin;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;

public class AbstractJobPlugin extends AbstractPlugin {
    //作业的config
    private Configuration pluginJobConf;

    // 修改为对端的作业configuration
    private Configuration peerPluginJobConf;

    private String peerPluginName;

    public Configuration getPluginJobConf() {
        return pluginJobConf;
    }

    public void setPluginJobConf(Configuration pluginJobConf) {
        this.pluginJobConf = pluginJobConf;
    }


    public Configuration getPeerPluginJobConf() {
        return peerPluginJobConf;
    }

    public void setPeerPluginJobConf(Configuration peerPluginJobConf) {
        this.peerPluginJobConf = peerPluginJobConf;
    }

    public String getPeerPluginName() {
        return peerPluginName;
    }

    public void setPeerPluginName(String peerPluginName) {
        this.peerPluginName = peerPluginName;
    }

    @Override
    public void init() {

    }

    @Override
    public void destroy() {

    }
}
