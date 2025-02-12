package com.jdragon.aggregation.report.stdout;

import com.jdragon.aggregation.core.plugin.spi.reporter.AbstractJobReporter;
import com.jdragon.aggregation.core.statistics.communication.RunStatus;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StdoutReporter extends AbstractJobReporter {
    @Override
    public void report(RunStatus runStatus) {
        log.info("ext report task status: {}", runStatus.getOtherReportInfo());
    }
}
