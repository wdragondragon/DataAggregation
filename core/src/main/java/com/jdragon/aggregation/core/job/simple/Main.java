package com.jdragon.aggregation.core.job.simple;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        ContinuousStream continuousStream = new ContinuousStream();

        // 注册 Transformer（转换器）
        continuousStream.registerTransformer(new UpperCaseTransformer());

        // 注册 Consumer（消费者）
        continuousStream.registerConsumer(new MessageConsumer());

        // 启动生产者（推送数据）
        continuousStream.startProducer(() -> {
            try {
                Thread.sleep(1000);  // 模拟生产延时
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "hello world";
        });

        // 启动消费者
        continuousStream.startConsumer();

        // 模拟运行一段时间
        Thread.sleep(5000);
        continuousStream.stop();
    }
}
