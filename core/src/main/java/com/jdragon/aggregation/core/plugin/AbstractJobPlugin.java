package com.jdragon.aggregation.core.plugin;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class AbstractJobPlugin extends AbstractPlugin {
    private String jobId;
    
    //作业的config
    private Configuration pluginJobConf;

    // 修改为对端的作业configuration
    private Configuration peerPluginJobConf;

    private String peerPluginName;

    private TaskPluginCollector taskPluginCollector;

    private JobPointReporter jobPointReporter;

    public void preCheck() {
    }

    public void prepare() {
    }

    public void post() {
    }

    public void preHandler(Configuration jobConfiguration) {

    }

    public void postHandler(Configuration jobConfiguration) {

    }
}
