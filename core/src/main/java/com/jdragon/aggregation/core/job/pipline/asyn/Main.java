package com.jdragon.aggregation.core.job.pipline.asyn;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        // 创建流管道
        Pipeline pipeline = new Pipeline(Arrays.asList(
                new Producer(() -> {
                    try {
                        Thread.sleep(1000);  // 模拟生产延时
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    System.out.println("生产");
                    return "hello world";
                }),
                new TransformerExec(
                        message -> {
                            System.out.println("加叹号");
                            message.setContent(message.getContent() + " !");  // 转换成大写
                            return message;
                        }
                )
        ));

        Pipeline pipelineSub = new Pipeline(Arrays.asList(
                new TransformerExec(
                        message -> {
                            System.out.println("转大写");
                            message.setContent(message.getContent().toUpperCase());
                            return message;
                        }
                ),
                new Consumer(message -> {
                    System.out.println("Consumed: " + message.getContent());
                })
        ));

        Pipeline all = new Pipeline(Arrays.asList(
                pipeline, pipelineSub
        ));

        // 启动流处理
        all.start();

        // 模拟运行一段时间
        Thread.sleep(100000);
        all.stop();
    }
}
