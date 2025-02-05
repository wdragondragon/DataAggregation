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
import com.jdragon.aggregation.core.taskgroup.runner.ReaderRunner;
import com.jdragon.aggregation.core.taskgroup.runner.WriterRunner;
import com.jdragon.aggregation.core.transformer.TransformerExecution;
import com.jdragon.aggregation.core.transport.channel.Channel;
import com.jdragon.aggregation.core.transport.channel.memory.MemoryChannel;
import com.jdragon.aggregation.core.transport.exchanger.BufferedRecordExchanger;
import com.jdragon.aggregation.core.transport.exchanger.BufferedRecordTransformerExchanger;
import com.jdragon.aggregation.core.utils.ClassUtil;
import com.jdragon.aggregation.core.utils.FrameworkErrorCode;
import com.jdragon.aggregation.core.utils.TransformerUtil;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.List;

@Slf4j
public class JobContainer {

    public static void main(String[] args) {
        Configuration configuration = Configuration.from(new File("C:\\dev\\ideaProject\\DataAggregation\\core\\src\\main\\resources\\job.json"));
        JobContainer container = new JobContainer();
        container.start(configuration);
    }

    public void start(Configuration configuration) {
        Communication jobCommunication = new Communication();
        Channel channel = new MemoryChannel();
        channel.setCommunication(jobCommunication);

        List<TransformerExecution> transformerExecutions = TransformerUtil.buildTransformerInfo(configuration);

        this.startJob(configuration, channel, jobCommunication, transformerExecutions);
        this.holdDoStat(jobCommunication, configuration);
    }


    private void startJob(Configuration configuration, Channel channel,
                          Communication jobCommunication, List<TransformerExecution> transformerExecutions) {
        Integer jobId = configuration.getInt("jobId", 1);
        Configuration reader = configuration.getConfiguration("reader");
        String readerType = reader.getString("type");
        Configuration readerConfiguration = reader.getConfiguration("config");

        Configuration writer = configuration.getConfiguration("writer");
        String writerType = writer.getString("type");
        Configuration writerConfiguration = writer.getConfiguration("config");

        String taskCollectorClass = configuration.getString("core.statistics.collector.plugin.taskClass",
                "com.jdragon.aggregation.core.plugin.StdoutPluginCollector");

        Thread readerThread, writerThread;
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.READER, readerType + PluginType.READER.getName())) {
            AbstractJobPlugin jobPlugin = classLoaderSwapper.loadPlugin();
            jobPlugin.setPluginJobConf(readerConfiguration);
            jobPlugin.setPeerPluginJobConf(writerConfiguration);
            AbstractTaskPluginCollector pluginCollector = ClassUtil.instantiate(
                    taskCollectorClass, AbstractTaskPluginCollector.class,
                    configuration, jobCommunication,
                    PluginType.READER);
            jobPlugin.setTaskPluginCollector(pluginCollector);

            ReaderRunner readerRunner = new ReaderRunner(jobPlugin);
            readerRunner.setJobId(jobId);
            readerRunner.setRecordSender(new BufferedRecordTransformerExchanger(channel, jobCommunication, pluginCollector, transformerExecutions));
            readerRunner.setRunnerCommunication(jobCommunication);

            readerThread = new Thread(readerRunner,
                    String.format("%d-reader", jobId));
            readerThread.setContextClassLoader(jobPlugin.getClassLoader());
        }
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.WRITER, writerType + PluginType.WRITER.getName())) {
            AbstractJobPlugin jobPlugin = classLoaderSwapper.loadPlugin();
            jobPlugin.setPluginJobConf(writerConfiguration);
            jobPlugin.setPeerPluginJobConf(readerConfiguration);
            AbstractTaskPluginCollector pluginCollector = ClassUtil.instantiate(
                    taskCollectorClass, AbstractTaskPluginCollector.class,
                    configuration, jobCommunication,
                    PluginType.WRITER);
            jobPlugin.setTaskPluginCollector(pluginCollector);

            WriterRunner writerRunner = new WriterRunner(jobPlugin);
            writerRunner.setJobId(jobId);
            writerRunner.setRecordReceiver(new BufferedRecordExchanger(channel));
            writerRunner.setRunnerCommunication(jobCommunication);

            writerThread = new Thread(writerRunner,
                    String.format("%d-writer", jobId));
            writerThread.setContextClassLoader(jobPlugin.getClassLoader());
        }

        readerThread.start();
        writerThread.start();

        jobCommunication.setState(State.RUNNING);
    }

    private void holdDoStat(Communication jobCommunication, Configuration configuration) {
        long reportIntervalInMillSec = configuration.getLong("core.container.taskGroup.reportInterval", 10000);
        int sleepIntervalInMillSec = configuration.getInt("core.container.taskGroup.sleepInterval", 100);
        long lastReportTimeStamp = 0;
        Communication lastTaskGroupContainerCommunication = new Communication();
        try {
            while (true) {
                if (jobCommunication.getState() == State.SUCCEEDED) {
                    reportTaskGroupCommunication(lastTaskGroupContainerCommunication, jobCommunication);
                    log.info("completed it's job.");
                    break;
                }
                // 5.如果当前时间已经超出汇报时间的interval，那么我们需要马上汇报
                long now = System.currentTimeMillis();
                if (now - lastReportTimeStamp > reportIntervalInMillSec) {
                    reportTaskGroupCommunication(
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

    private void reportTaskGroupCommunication(Communication lastTaskGroupContainerCommunication, Communication nowCommunication) {
        Communication nowTaskGroupContainerCommunication = nowCommunication.clone();
        nowTaskGroupContainerCommunication.setTimestamp(System.currentTimeMillis());
        Communication reportCommunication = CommunicationTool.getReportCommunication(nowTaskGroupContainerCommunication,
                lastTaskGroupContainerCommunication);
        log.info(CommunicationTool.Stringify.getSnapshot(reportCommunication));
    }
}
