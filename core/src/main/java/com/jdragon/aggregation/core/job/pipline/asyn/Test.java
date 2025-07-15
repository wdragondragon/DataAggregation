package com.jdragon.aggregation.core.job.pipline.asyn;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;

public class Test {
    public static void main(String[] args) throws InterruptedException {
//        serial();
        merge();
    }

    private static void serial() throws InterruptedException {
        // 串行流
        Pipeline all = new Pipeline(
                new Pipeline(
                        new Producer(() -> {
                            DefaultRecord defaultRecord = new DefaultRecord();
                            defaultRecord.setColumn(0, new StringColumn("Hello world"));
                            return defaultRecord;
                        }),
                        new TransformerExec(
                                message -> {
                                    Column column = message.getColumn(0);
                                    message.setColumn(0, new StringColumn(column.asString() + " !"));
                                    return message;
                                }
                        )
                ),
                new Pipeline(
                        new TransformerExec(
                                message -> {
                                    Column column = message.getColumn(0);
                                    message.setColumn(0, new StringColumn(column.asString().toUpperCase()));
                                    return message;
                                }
                        ),
                        new Consumer(message -> System.out.println("Consumed: " + message.getColumn(0).asString()))
                )
        );

        // 启动流处理
        all.start();

        // 模拟运行一段时间
        Thread.sleep(1000);
        all.stop();
    }

    private static void merge() throws InterruptedException {
        Pipeline all = new Pipeline(
                new MergePipeline(
                        new Pipeline(
                                new Producer(() -> {
                                    DefaultRecord defaultRecord = new DefaultRecord();
                                    defaultRecord.setColumn(0, new StringColumn("Hello world"));
                                    return defaultRecord;
                                }),
                                new TransformerExec(
                                        message -> {
                                            message.setColumn(0, new StringColumn("1"));
                                            return message;
                                        }
                                )
                        ),
                        new Pipeline(
                                new Producer(() -> {
                                    DefaultRecord defaultRecord = new DefaultRecord();
                                    defaultRecord.setColumn(0, new StringColumn("你好"));
                                    return defaultRecord;
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
                new Consumer(message -> System.out.println("Consumed: " + message.getColumn(0).asString()))
        );

        // 启动流处理
        all.start();

        // 模拟运行一段时间
        Thread.sleep(1000);
        all.stop();
    }
}
