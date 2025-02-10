package com.jdragon.aggregation.core.job;

import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.statistics.VMInfo;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.enums.State;
import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.spi.collector.AbstractTaskPluginCollector;
import com.jdragon.aggregation.core.statistics.communication.Communication;
import com.jdragon.aggregation.core.statistics.communication.CommunicationTool;
import com.jdragon.aggregation.core.taskgroup.runner.AbstractRunner;
import com.jdragon.aggregation.core.taskgroup.runner.ReaderRunner;
import com.jdragon.aggregation.core.taskgroup.runner.WriterRunner;
import com.jdragon.aggregation.core.transformer.ParamsKey;
import com.jdragon.aggregation.core.transformer.TransformerExecution;
import com.jdragon.aggregation.core.transport.channel.Channel;
import com.jdragon.aggregation.core.transport.channel.memory.MemoryChannel;
import com.jdragon.aggregation.core.transport.exchanger.BufferedRecordExchanger;
import com.jdragon.aggregation.core.transport.exchanger.BufferedRecordTransformerExchanger;
import com.jdragon.aggregation.core.utils.ClassUtil;
import com.jdragon.aggregation.core.utils.FrameworkErrorCode;
import com.jdragon.aggregation.core.utils.TransformerUtil;
import com.jdragon.aggregation.pluginloader.ClassLoaderSwapper;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import com.jdragon.aggregation.pluginloader.type.IPluginType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.List;
import java.util.Set;

@Slf4j
public class JobContainer {

    public static void main(String[] args) {
        Configuration configuration = Configuration.from(new File("C:\\dev\\ideaProject\\DataAggregation\\core\\src\\main\\resources\\kafkareader.json"));
        JobContainer container = new JobContainer();
        container.start(configuration);
    }

    public void start(Configuration configuration) {
        log.info("start job from configuration: {}", filterSensitiveConfiguration(configuration.clone()).beautify());
        // 初始化全局channel 和 communication
        Communication jobCommunication = new Communication();
        Channel channel = new MemoryChannel();
        channel.setCommunication(jobCommunication);

        // 启动作业
        this.startJob(configuration, channel, jobCommunication);

        // 持续输出作业状态
        this.holdDoStat(jobCommunication, configuration);
    }

    private void startJob(Configuration configuration, Channel channel,
                          Communication jobCommunication) {
        Integer jobId = configuration.getInt("jobId", 1);
        Configuration reader = configuration.getConfiguration("reader");
        String readerType = reader.getString("type");
        Configuration readerConfiguration = reader.getConfiguration("config");

        Configuration writer = configuration.getConfiguration("writer");
        String writerType = writer.getString("type");
        Configuration writerConfiguration = writer.getConfiguration("config");

        String taskCollectorClass = configuration.getString("core.statistics.collector.plugin.taskClass",
                "com.jdragon.aggregation.core.plugin.StdoutPluginCollector");

        List<TransformerExecution> transformerExecutions = TransformerUtil.buildTransformerInfo(configuration);

        AbstractJobPlugin readerJobPlugin = initJobPlugin(PluginType.READER, readerType, readerConfiguration, writerConfiguration);
        AbstractJobPlugin writerJobPlugin = initJobPlugin(PluginType.WRITER, writerType, writerConfiguration, readerConfiguration);

        Thread readerThread = initExecThread(jobId, readerJobPlugin, transformerExecutions, taskCollectorClass, jobCommunication, channel);
        Thread writerThread = initExecThread(jobId, writerJobPlugin, transformerExecutions, taskCollectorClass, jobCommunication, channel);

        jobCommunication.setState(State.RUNNING);

        // 先确保执行init成功，再执行start
        ClassLoaderSwapper classLoaderSwapper = ClassLoaderSwapper.newCurrentThreadClassLoaderSwapper();
        classLoaderSwapper.setCurrentThreadClassLoader(writerJobPlugin.getClassLoader());
        try {
            log.info("job writer init start");
            writerJobPlugin.init();
            log.info("job writer init end");
        } catch (Throwable e) {
            throw AggregationException.asException(FrameworkErrorCode.RUNTIME_ERROR, "job writer init error", e);
        } finally {
            classLoaderSwapper.restoreCurrentThreadClassLoader();
        }

        classLoaderSwapper.setCurrentThreadClassLoader(readerJobPlugin.getClassLoader());
        try {
            log.info("job reader init start");
            readerJobPlugin.init();
            log.info("job reader init end");
        } catch (Throwable e) {
            throw AggregationException.asException(FrameworkErrorCode.RUNTIME_ERROR, "job reader init error", e);
        } finally {
            classLoaderSwapper.restoreCurrentThreadClassLoader();
        }
        log.info("plugin init finish");

        writerThread.start();
        // reader没有起来，writer不可能结束
        if (!writerThread.isAlive() || jobCommunication.getState() == State.FAILED) {
            throw AggregationException.asException(
                    FrameworkErrorCode.RUNTIME_ERROR,
                    jobCommunication.getThrowable());
        }
        readerThread.start();
        // 这里reader可能很快结束
        if (!readerThread.isAlive() && jobCommunication.getState() == State.FAILED) {
            // 这里有可能出现Reader线上启动即挂情况 对于这类情况 需要立刻抛出异常
            throw AggregationException.asException(
                    FrameworkErrorCode.RUNTIME_ERROR,
                    jobCommunication.getThrowable());
        }
    }

    private AbstractJobPlugin initJobPlugin(PluginType pluginType, String pluginName,
                                            Configuration configuration, Configuration peerConfiguration) {
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(pluginType, pluginName + pluginType.getName())) {
            AbstractJobPlugin jobPlugin = classLoaderSwapper.loadPlugin();
            jobPlugin.setPluginJobConf(configuration);
            jobPlugin.setPeerPluginJobConf(peerConfiguration);
            return jobPlugin;
        }
    }

    private Thread initExecThread(Integer jobId, AbstractJobPlugin jobPlugin,
                                  List<TransformerExecution> transformerExecutions, String taskCollectorClass,
                                  Communication jobCommunication, Channel channel) {
        IPluginType pluginType = jobPlugin.getPluginType();
        Configuration configuration = jobPlugin.getPluginJobConf();
        AbstractTaskPluginCollector pluginCollector = ClassUtil.instantiate(
                taskCollectorClass, AbstractTaskPluginCollector.class,
                configuration, jobCommunication,
                pluginType);
        jobPlugin.setTaskPluginCollector(pluginCollector);

        AbstractRunner runner;
        if (pluginType == PluginType.READER) {
            ReaderRunner readerRunner = new ReaderRunner(jobPlugin);
            readerRunner.setRecordSender(new BufferedRecordTransformerExchanger(channel, jobCommunication, pluginCollector, transformerExecutions));
            runner = readerRunner;
        } else {
            WriterRunner writerRunner = new WriterRunner(jobPlugin);
            writerRunner.setRecordReceiver(new BufferedRecordExchanger(channel));
            runner = writerRunner;
        }
        runner.setJobId(jobId);
        runner.setRunnerCommunication(jobCommunication);
        Thread runThread = new Thread(runner,
                String.format("%d-%s", jobId, pluginType.getName()));
        runThread.setContextClassLoader(jobPlugin.getClassLoader());
        return runThread;
    }

    private void holdDoStat(Communication jobCommunication, Configuration configuration) {
        long reportIntervalInMillSec = configuration.getLong("core.container.taskGroup.reportInterval", 10000);
        int sleepIntervalInMillSec = configuration.getInt("core.container.taskGroup.sleepInterval", 100);
        long lastReportTimeStamp = 0;
        Communication lastTaskGroupContainerCommunication = new Communication();
        try {
            while (true) {
                if (jobCommunication.isFinished()) {
                    reportTaskGroupCommunication(lastTaskGroupContainerCommunication, jobCommunication);
                    log.info("completed it's job. status is {}", jobCommunication.getState());
                    break;
                }
                // 5.如果当前时间已经超出汇报时间的interval，那么我们需要马上汇报
                long now = System.currentTimeMillis();
                if (now - lastReportTimeStamp > reportIntervalInMillSec) {
                    lastTaskGroupContainerCommunication = reportTaskGroupCommunication(
                            lastTaskGroupContainerCommunication, jobCommunication);
                    lastReportTimeStamp = now;
                }

                Thread.sleep(sleepIntervalInMillSec);
            }
        } catch (Throwable e) {
            if (jobCommunication.getThrowable() == null) {
                jobCommunication.setThrowable(e);
            }
            jobCommunication.setState(State.FAILED);
            log.info(CommunicationTool.Stringify.getSnapshot(jobCommunication));
            throw AggregationException.asException(
                    FrameworkErrorCode.RUNTIME_ERROR, e);
        } finally {
            //最后打印cpu的平均消耗，GC的统计
            VMInfo vmInfo = VMInfo.getVmInfo();
            if (vmInfo != null) {
                vmInfo.getDelta(false);
                log.info(vmInfo.totalString());
            }
        }
    }

    private Communication reportTaskGroupCommunication(Communication lastTaskGroupContainerCommunication, Communication nowCommunication) {
        Communication nowTaskGroupContainerCommunication = nowCommunication.clone();
        nowTaskGroupContainerCommunication.setTimestamp(System.currentTimeMillis());
        Communication reportCommunication = CommunicationTool.getReportCommunication(nowTaskGroupContainerCommunication,
                lastTaskGroupContainerCommunication);
        log.info(CommunicationTool.Stringify.getSnapshot(reportCommunication));
        return reportCommunication;
    }

    public static Configuration filterSensitiveConfiguration(Configuration configuration) {
        Set<String> keys = configuration.getKeys();
        for (final String key : keys) {
            boolean isSensitive = StringUtils.endsWithIgnoreCase(key, "password")
                    || StringUtils.endsWithIgnoreCase(key, "accessKey");
            if (isSensitive && configuration.get(key) instanceof String) {
                configuration.set(key, configuration.getString(key).replaceAll(".", "*"));
            }
        }
        return configuration;
    }
}
