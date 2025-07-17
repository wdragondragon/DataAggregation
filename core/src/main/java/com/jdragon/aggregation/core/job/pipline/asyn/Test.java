package com.jdragon.aggregation.core.job.pipline.asyn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.job.pipline.asyn.config.PipelineBuilder;
import com.jdragon.aggregation.core.job.pipline.asyn.config.PipelineConfig;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class Test {
    public static void main(String[] args) throws InterruptedException, IOException {
//        serial();
//        merge();
        broadcast();
//        test();
    }

    private static void serial() throws InterruptedException {
        // 串行流
        Writer.Job writer;
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.WRITER, "consolewriter")) {
            writer = classLoaderSwapper.loadPlugin();
        }
        Reader.Job reader;
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.READER, "consolereader")) {
            reader = classLoaderSwapper.loadPlugin();
        }
        Configuration readConf = Configuration.newDefault();
        readConf.set("rowCount", 10);
        reader.setPluginJobConf(readConf);
        reader.init();

        Pipeline all = new Pipeline("pip-all",
                new Producer(reader),
                new TransformerExec(
                        message -> {
                            Column column = message.getColumn(0);
                            message.setColumn(0, new StringColumn(column.asString() + " !"));
                            return message;
                        },
                        message -> {
                            Column column = message.getColumn(0);
                            message.setColumn(0, new StringColumn(column.asString().toUpperCase()));
                            return message;
                        }
                ),
                new Consumer(writer)
        );

        // 启动流处理
        all.start();
    }

    private static void merge() throws InterruptedException {
        Writer.Job writer;
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.WRITER, "consolewriter")) {
            writer = classLoaderSwapper.loadPlugin();
        }
        Reader.Job reader;
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.READER, "consolereader")) {
            reader = classLoaderSwapper.loadPlugin();
        }
        Configuration readConf = Configuration.newDefault();
        readConf.set("rowCount", 10);
        reader.setPluginJobConf(readConf);
        reader.init();

        Pipeline all = new Pipeline("pip-all",
                new MergePipeline("pip-merge",
                        new Producer(reader),
                        new Pipeline("pip-2",
                                new Producer(new Reader.Job() {
                                    final AtomicInteger count = new AtomicInteger(0);

                                    @Override
                                    public void startRead(RecordSender recordSender) {
                                        while (count.incrementAndGet() <= 10) {
                                            log.info("pip-1 count : {}", count.get());
                                            DefaultRecord defaultRecord = new DefaultRecord();
                                            defaultRecord.setColumn(0, new StringColumn("你好" + count.get()));
                                            recordSender.sendToWriter(defaultRecord);
                                        }
                                    }
                                }),
                                new TransformerExec(
                                        message -> {
                                            Column column = message.getColumn(0);
                                            message.setColumn(0, new StringColumn(column.asString() + " !"));
                                            return message;
                                        }
                                )
                        )
                ),
                new Consumer(writer)
        );

        // 启动流处理
        all.start();
    }


    public static void broadcast() throws InterruptedException {
        Writer.Job writer1;
        Writer.Job writer2;
        Reader.Job reader;
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.WRITER, "consolewriter")) {
            writer1 = classLoaderSwapper.loadPlugin();
        }
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.WRITER, "consolewriter")) {
            writer2 = classLoaderSwapper.loadPlugin();
        }
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.READER, "consolereader")) {
            reader = classLoaderSwapper.loadPlugin();
        }

        Configuration readConf = Configuration.newDefault();
        readConf.set("rowCount", 10);
        reader.setPluginJobConf(readConf);
        reader.init();

        Pipeline pipeline = new Pipeline("pipeline",
                new MergePipeline("merge-pipeline",
                        new Pipeline("producer-custom",
                                new Producer(new Reader.Job() {
                                    final AtomicInteger count = new AtomicInteger(0);

                                    @Override
                                    public void startRead(RecordSender recordSender) {
                                        while (count.incrementAndGet() <= 10) {
                                            DefaultRecord defaultRecord = new DefaultRecord();
                                            defaultRecord.setColumn(0, new StringColumn("你好" + count.get()));
                                            recordSender.sendToWriter(defaultRecord);
                                        }
                                    }
                                }),
                                new TransformerExec(
                                        message -> {
                                            Column column = message.getColumn(0);
                                            message.setColumn(0, new StringColumn(column.asString() + " !"));
                                            return message;
                                        }
                                )
                        ),
                        new Producer(reader)
                ),
                new BroadcastPipeline("broadcast-pipeline",
                        new Pipeline("consumer-custom",
                                new TransformerExec(
                                        message -> {
                                            Column column = message.getColumn(0);
                                            message.setColumn(0, new StringColumn(column.asString() + " @"));
                                            return message;
                                        }
                                ),
                                new Consumer(writer1)
                        ),
                        new Pipeline("sub-pipe",
                                new TransformerExec(
                                        message -> {
                                            Column column = message.getColumn(0);
                                            message.setColumn(0, new StringColumn(column.asString() + " ？"));
                                            return message;
                                        }
                                ),
                                new BroadcastPipeline("sub-pipe-broadcast",
                                        new Consumer(writer2),
                                        new Consumer(new Writer.Job() {
                                            @Override
                                            public void startWrite(RecordReceiver lineReceiver) {
                                                Record record;
                                                while ((record = lineReceiver.getFromReader()) != null) {
                                                    log.info("consumer-custom:[{}]", record);
                                                }
                                            }
                                        }))
                        )
                )
        );

        pipeline.start();

        while (pipeline.isRunning()) {
            Thread.sleep(500);
            log.info("broadcast pipeline running...");
        }

        log.info("broadcast pipeline finished.");

        pipeline.stop();
    }

    public static void test() throws InterruptedException, IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        PipelineConfig config = mapper.readValue(new File("C:\\dev\\ideaProject\\DataAggregation\\core\\src\\main\\resources\\pipeline2.yaml"), PipelineConfig.class);

        PipelineAbstract handler = PipelineBuilder.buildPipeline(config);
        handler.start();
    }
}
