package com.jdragon.aggregation.core.job;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.taskgroup.runner.ReaderRunner;
import com.jdragon.aggregation.core.taskgroup.runner.WriterRunner;
import com.jdragon.aggregation.core.transformer.TransformerExecution;
import com.jdragon.aggregation.core.transport.channel.Channel;
import com.jdragon.aggregation.core.transport.channel.memory.MemoryChannel;
import com.jdragon.aggregation.core.transport.exchanger.BufferedRecordExchanger;
import com.jdragon.aggregation.core.transport.exchanger.BufferedRecordTransformerExchanger;
import com.jdragon.aggregation.core.utils.TransformerUtil;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;

import java.io.File;
import java.util.List;

public class JobContainer {

    public static void main(String[] args) {
        Configuration configuration = Configuration.from(new File("C:\\dev\\ideaProject\\DataAggregation\\core\\src\\main\\resources\\job.json"));
        JobContainer container = new JobContainer();
        container.start(configuration);
    }

    public void start(Configuration configuration) {
        Integer jobId = configuration.getInt("jobId", 1);
        Configuration reader = configuration.getConfiguration("reader");
        String readerType = reader.getString("type");
        Configuration readerConfiguration = reader.getConfiguration("config");

        Configuration writer = configuration.getConfiguration("writer");
        String writerType = writer.getString("type");
        Configuration writerConfiguration = writer.getConfiguration("config");

        Channel channel = new MemoryChannel();

        List<TransformerExecution> transformerExecutions = TransformerUtil.buildTransformerInfo(configuration);

        Thread readerThread, writerThread;
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.READER, readerType + PluginType.READER.getName())) {
            AbstractJobPlugin jobPlugin = classLoaderSwapper.loadPlugin();
            jobPlugin.setPluginJobConf(readerConfiguration);
            jobPlugin.setPeerPluginJobConf(writerConfiguration);

            ReaderRunner readerRunner = new ReaderRunner(jobPlugin);

            readerRunner.setRecordSender(new BufferedRecordTransformerExchanger(channel, transformerExecutions));
            readerThread = new Thread(readerRunner,
                    String.format("%d-reader", jobId));
            readerThread.setContextClassLoader(jobPlugin.getClassLoader());
        }
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.WRITER, writerType + PluginType.WRITER.getName())) {
            AbstractJobPlugin jobPlugin = classLoaderSwapper.loadPlugin();
            jobPlugin.setPluginJobConf(writerConfiguration);
            jobPlugin.setPeerPluginJobConf(readerConfiguration);

            WriterRunner writerRunner = new WriterRunner(jobPlugin);
            writerRunner.setRecordReceiver(new BufferedRecordExchanger(channel));
            writerThread = new Thread(writerRunner,
                    String.format("%d-writer", jobId));
            writerThread.setContextClassLoader(jobPlugin.getClassLoader());
        }

        readerThread.start();
        writerThread.start();
    }
}
