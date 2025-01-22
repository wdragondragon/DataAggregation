package com.jdragon.aggregation.core.taskgroup.runner;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import lombok.Data;

@Data
public class AbstractRunner {
    private AbstractJobPlugin plugin;

    private Configuration jobConf;

    private int taskGroupId;

    private int taskId;

    public AbstractRunner(AbstractJobPlugin plugin) {
        this.plugin = plugin;
    }

    public void setJobConf(Configuration jobConf) {
        this.jobConf = jobConf;
        this.plugin.setPluginJobConf(jobConf);
    }

    public void destroy() {
        if (this.plugin != null) {
            this.plugin.destroy();
        }
    }
}
