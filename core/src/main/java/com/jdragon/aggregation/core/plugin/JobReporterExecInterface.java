package com.jdragon.aggregation.core.plugin;

import com.jdragon.aggregation.core.statistics.communication.RunStatus;

@FunctionalInterface
public interface JobReporterExecInterface {

    void report(RunStatus runStatus);

}
