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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

@Slf4j
@Setter
@Getter
public class JobPointReporter implements Runnable {

    private long jobId;

    private BlockingQueue<RunStatus> fileStorageQueue = new LinkedBlockingDeque<>();

    private Communication trackCommunication;

    private List<JobReporterExecutor> jobReporterExecInterfaceList = new LinkedList<>();

    private Configuration configuration;

    private Map<String, Object> otherReportInfo = new ConcurrentHashMap<>();

    public JobPointReporter(Configuration configuration) {
        loadReporter(configuration);
        this.jobId = configuration.getLong("jobId", 1L);
        this.configuration = configuration;
    }

    private void loadReporter(Configuration configuration) {
        List<Configuration> reporterInfos = configuration.getListConfiguration(Key.REPORT_CLASS);
        for (Configuration subConfig : reporterInfos) {
            ReporterInfo reporterInfo = JSONObject.parseObject(subConfig.toJSON(), ReporterInfo.class);
            AbstractJobReporter jobReporter;
            String className = reporterInfo.getClassName();
            if (StringUtils.isNotBlank(className)) {
                jobReporter = ClassUtil.instantiate(
                        className, AbstractJobReporter.class, configuration);
                jobReporter.setClassLoader(Thread.currentThread().getContextClassLoader());
            } else {
                String name = reporterInfo.getName();
                try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.REPORT, name)) {
                    jobReporter = classLoaderSwapper.loadPlugin();
                    jobReporter.setConfiguration(configuration);
                }
            }
            reg(jobReporter);
        }
    }

    public void put(String key, Object value) {
        otherReportInfo.put(key, value);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key, Object defaultValue) {
        return (T) otherReportInfo.getOrDefault(key, defaultValue);
    }

    public RunStatus recovery() {
        RunStatus runStatus = openReport();
        jobReporterExecInterfaceList.forEach(jobReporterExecInterface -> jobReporterExecInterface.recovery(runStatus));
        return runStatus;
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
        this.report(new RunStatus(CommunicationTool.getReportCommunication(now, start),this));
    }

    private void report(RunStatus runStatus) {
        this.fileStorageQueue.add(runStatus);
    }

    public void reg(JobReporterExecInterface jobReporterExecInterface) {
        reg(new BaseJobReporter(jobReporterExecInterface, configuration));
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
