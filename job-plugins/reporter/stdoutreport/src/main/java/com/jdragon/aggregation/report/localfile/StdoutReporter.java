package com.jdragon.aggregation.report.localfile;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.spi.reporter.AbstractJobReporter;
import com.jdragon.aggregation.core.statistics.communication.RunStatus;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StdoutReporter extends AbstractJobReporter {
    public StdoutReporter(Configuration configuration) {
        super(configuration);
    }

    @Override
    public void report(RunStatus runStatus) {
        log.info("ext report task status: {}", runStatus.getOtherReportInfo());
    }

    @Override
    public void recovery(RunStatus runStatus) {

    }
}
