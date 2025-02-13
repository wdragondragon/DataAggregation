package com.jdragon.aggregation.core.plugin.spi.reporter;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.statistics.communication.RunStatus;

public class BaseJobReporter extends AbstractJobReporter {

    private final JobReporterExecInterface jobReporterExecInterface;

    public BaseJobReporter(JobReporterExecInterface jobReporterExecInterface, Configuration configuration) {
        super(configuration);
        this.jobReporterExecInterface = jobReporterExecInterface;
    }

    @Override
    public void report(RunStatus runStatus) {
        jobReporterExecInterface.report(runStatus);
    }

    @Override
    public void recovery(RunStatus runStatus) {

    }

}
