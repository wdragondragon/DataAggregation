package com.jdragon.aggregation.core.plugin.spi.reporter;

import com.jdragon.aggregation.core.statistics.communication.RunStatus;

public class BaseJobReporter extends AbstractJobReporter {

    private final JobReporterExecInterface jobReporterExecInterface;

    public BaseJobReporter(JobReporterExecInterface jobReporterExecInterface) {
        this.jobReporterExecInterface = jobReporterExecInterface;
    }

    @Override
    public void report(RunStatus runStatus) {
        jobReporterExecInterface.report(runStatus);
    }
}
