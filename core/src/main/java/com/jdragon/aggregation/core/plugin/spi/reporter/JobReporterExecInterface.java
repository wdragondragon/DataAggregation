package com.jdragon.aggregation.core.plugin.spi.reporter;

import com.jdragon.aggregation.core.statistics.communication.RunStatus;

public interface JobReporterExecInterface {

    void report(RunStatus runStatus);

    void recovery(RunStatus runStatus);
}
