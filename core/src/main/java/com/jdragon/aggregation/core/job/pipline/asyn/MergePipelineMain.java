package com.jdragon.aggregation.core.job.pipline.asyn;

public class MergePipelineMain {
    public static void main(String[] args) throws InterruptedException {
        // 创建流管道
        Pipeline pipeline = new Pipeline(
                new Producer(() -> {
                    try {
                        Thread.sleep(1000);  // 模拟生产延时
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "hello world";
                }),
                new TransformerExec(
                        message -> {
                            message.setContent(message.getContent() + " !");  // 转换成大写
                            return message;
                        }
                )
        );

        Pipeline pipelineSub = new Pipeline(
                new Producer(() -> {
                    try {
                        Thread.sleep(1000);  // 模拟生产延时
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return "你好";
                }),
                new TransformerExec(
                        message -> {
                            message.setContent(message.getContent() + " ！");
                            return message;
                        }
                )
        );

        MergePipeline mergePipeline = new MergePipeline(
                pipeline, pipelineSub
        );

        Pipeline all = new Pipeline(
                mergePipeline,
                new Consumer(message -> {
                    System.out.println("Consumed: " + message.getContent());
                })
        );


        // 启动流处理
        all.start();

        // 模拟运行一段时间
        Thread.sleep(5000);
        all.stop();
    }
}
