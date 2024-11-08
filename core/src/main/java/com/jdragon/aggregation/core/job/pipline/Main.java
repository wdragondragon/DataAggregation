package com.jdragon.aggregation.core.job.pipline;

import java.util.Arrays;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        // 创建流处理器列表（可以自由配置顺序）
        Pipeline pipeline = new Pipeline(Arrays.asList(
                new StreamProducer(() -> {
                    try {
                        Thread.sleep(1000);  // 模拟生产延时
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "hello world";
                }),
                new Transformer(message -> {
                    message.setContent(message.getContent().toUpperCase());  // 转换成大写
                    return message;
                }),
                new StreamConsumer(message -> {
                    System.out.println("Consumed: " + message.getContent());
                })
        ));

        // 启动流处理
        pipeline.start();

        // 模拟运行一段时间
        Thread.sleep(5000);
        pipeline.stop();
    }
}
