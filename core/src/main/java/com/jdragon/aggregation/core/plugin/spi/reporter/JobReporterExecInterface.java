package com.jdragon.aggregation.core.plugin.spi.reporter;

import com.jdragon.aggregation.core.statistics.communication.RunStatus;

@FunctionalInterface
public interface JobReporterExecInterface {

    void report(RunStatus runStatus);

}
