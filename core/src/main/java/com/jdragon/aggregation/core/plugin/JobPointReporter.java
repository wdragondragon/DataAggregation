package com.jdragon.aggregation.core.plugin;

import com.jdragon.aggregation.core.statistics.communication.Communication;
import com.jdragon.aggregation.core.statistics.communication.CommunicationTool;
import com.jdragon.aggregation.core.statistics.communication.RunStatus;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

@Setter
@Getter
public class JobPointReporter {

    private ExecutorService executor;

    private Communication trackCommunication;

    private List<JobReporterExecInterface> jobReporterExecInterfaceList = new LinkedList<>();

    public RunStatus openReport() {
        return new RunStatus(this);
    }

    public void report() {
        if (trackCommunication != null) {
            this.report(trackCommunication);
        }
    }

    public void report(Communication communication) {
        Communication now = communication.clone();
        now.setTimestamp(System.currentTimeMillis());
        Communication start = new Communication();
        start.setTimestamp(communication.getTimestamp());
        this.report(new RunStatus(CommunicationTool.getReportCommunication(now, start)));
    }

    private void report(RunStatus runStatus) {
        jobReporterExecInterfaceList.forEach(jobReporterExecInterface -> jobReporterExecInterface.report(runStatus));
    }

    public void reg(JobReporterExecInterface jobReporterExecInterface) {
        jobReporterExecInterfaceList.add(jobReporterExecInterface);
    }
}
