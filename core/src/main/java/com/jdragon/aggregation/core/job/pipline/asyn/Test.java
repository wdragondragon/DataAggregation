package com.jdragon.aggregation.core.job.pipline.asyn;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.core.transport.record.TerminateRecord;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Slf4j
public class Test {
    public static void main(String[] args) throws InterruptedException {
        serial();
//        merge();
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
                new Pipeline("pip-1",
                        new Producer(reader),
                        new TransformerExec(
                                message -> {
                                    Column column = message.getColumn(0);
                                    message.setColumn(0, new StringColumn(column.asString() + " !"));
                                    return message;
                                }
                        )
                ),
                new Pipeline("pip-2",
                        new TransformerExec(
                                message -> {
                                    Column column = message.getColumn(0);
                                    message.setColumn(0, new StringColumn(column.asString().toUpperCase()));
                                    return message;
                                }
                        ),
                        new Consumer(writer)
                )
        );

        // 启动流处理
        all.start();

        while (all.isRunning()) {
            Thread.sleep(100);
        }
        log.info("pipeline end...");
        // 模拟运行一段时间
        Thread.sleep(1000);
        all.stop();
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
                                new Producer(new Supplier<Record>() {
                                    final AtomicInteger count = new AtomicInteger(0);

                                    @Override
                                    public Record get() {
                                        if (count.incrementAndGet() <= 10) {
                                            log.info("pip-1 count : {}", count.get());
                                            DefaultRecord defaultRecord = new DefaultRecord();
                                            defaultRecord.setColumn(0, new StringColumn("你好" + count.get()));
                                            return defaultRecord;
                                        } else {
                                            return TerminateRecord.get();
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

        while (all.isRunning()) {
            Thread.sleep(1000);
            log.info("pipeline running...");
        }
        log.info("pipeline end...");
        // 模拟运行一段时间
        Thread.sleep(1000);
        all.stop();
    }
}
