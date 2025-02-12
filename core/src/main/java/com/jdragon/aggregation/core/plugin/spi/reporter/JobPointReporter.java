package com.jdragon.aggregation.core.plugin.spi.reporter;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.enums.Key;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.statistics.communication.Communication;
import com.jdragon.aggregation.core.statistics.communication.CommunicationTool;
import com.jdragon.aggregation.core.statistics.communication.RunStatus;
import com.jdragon.aggregation.core.utils.ClassUtil;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

@Slf4j
@Setter
@Getter
public class JobPointReporter extends Thread {

    private BlockingQueue<RunStatus> fileStorageQueue = new LinkedBlockingDeque<>();

    private Communication trackCommunication;

    private List<JobReporterExecutor> jobReporterExecInterfaceList = new LinkedList<>();

    public JobPointReporter(Configuration configuration) {
        loadReporter(configuration);
    }

    private void loadReporter(Configuration configuration) {
        List<Configuration> reporterInfos = configuration.getListConfiguration(Key.REPORT_CLASS);
        for (Configuration subConfig : reporterInfos) {
            ReporterInfo reporterInfo = JSONObject.parseObject(subConfig.toJSON(), ReporterInfo.class);
            AbstractJobReporter jobReporter;
            String className = reporterInfo.getClassName();
            if (StringUtils.isNotBlank(className)) {
                jobReporter = ClassUtil.instantiate(
                        className, AbstractJobReporter.class);
                jobReporter.setClassLoader(Thread.currentThread().getContextClassLoader());
            } else {
                String name = reporterInfo.getName();
                try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.REPORT, name)) {
                    jobReporter = classLoaderSwapper.loadPlugin();
                }
            }
            reg(jobReporter);
        }
    }

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
        this.fileStorageQueue.add(runStatus);
    }

    public void reg(JobReporterExecInterface jobReporterExecInterface) {
        reg(new BaseJobReporter(jobReporterExecInterface));
    }

    public void reg(AbstractJobReporter reporter) {
        jobReporterExecInterfaceList.add(new JobReporterExecutor(reporter));
    }

    @Override
    public void run() {
        try {
            log.info("任务上报线程开始执行");
            while (true) {
                RunStatus runStatus = fileStorageQueue.take();
                jobReporterExecInterfaceList.forEach(jobReporterExecInterface -> jobReporterExecInterface.exec(runStatus));
                if (trackCommunication.isFinished()) {
                    log.info("任务上报线程结束");
                    break;
                }
            }
        } catch (InterruptedException e) {
            log.error("任务上报线程消费失败：{}", e.getMessage(), e);
        }
    }
}
