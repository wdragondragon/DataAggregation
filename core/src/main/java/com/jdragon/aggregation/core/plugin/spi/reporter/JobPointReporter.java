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

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

@Slf4j
@Setter
@Getter
public class JobPointReporter implements Runnable {

    private Map<String, Object> runContext = new HashMap<>();

    private BlockingQueue<RunStatus> fileStorageQueue = new LinkedBlockingDeque<>();

    private Communication trackCommunication;

    private List<JobReporterExecutor> jobReporterExecInterfaceList = new LinkedList<>();

    private Configuration configuration;

    private Map<String, Object> otherReportInfo = new ConcurrentHashMap<>();

    private static final List<AbstractJobReporter> globalReporters = new LinkedList<>();

    public JobPointReporter(Configuration configuration, Map<String, Object> runContext) {
        this.runContext = runContext;
        this.configuration = configuration;
    }

    public void loadReporter(Configuration configuration) {
        List<Configuration> reporterInfos = configuration.getListConfiguration(Key.REPORT_CLASS);
        if (reporterInfos == null) {
            reporterInfos = new ArrayList<>();
        }
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
        globalReporters.forEach(this::reg);
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
        RunStatus runStatus = new RunStatus(this);
        runStatus.setRunContext(runContext);
        return runStatus;
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
        this.report(new RunStatus(CommunicationTool.getReportCommunication(now, start), this));
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

    public static void regGlobal(AbstractJobReporter reporter) {
        globalReporters.add(reporter);
    }

    @Override
    public void run() {
        try {
            log.info("任务上报线程开始执行");
            while (true) {
                RunStatus runStatus = fileStorageQueue.take();
                runStatus.setRunContext(runContext);
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
