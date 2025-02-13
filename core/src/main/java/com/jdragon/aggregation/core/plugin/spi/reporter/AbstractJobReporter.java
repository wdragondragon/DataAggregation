package com.jdragon.aggregation.core.plugin.spi.reporter;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import lombok.Getter;

@Getter
public abstract class AbstractJobReporter extends AbstractPlugin implements JobReporterExecInterface {

    private Long jobId;

    private Configuration configuration;

    public AbstractJobReporter() {
    }

    public AbstractJobReporter(Configuration configuration) {
        setConfiguration(configuration);
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
        this.jobId = configuration.getLong("jobId", 1L);
    }
}
