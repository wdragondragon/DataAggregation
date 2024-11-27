package com.jdragon.aggregation.core.job.pipline.asyn;

public class MergePipelineMain {
    public static void main(String[] args) throws InterruptedException {
        Pipeline all = new Pipeline(
                new MergePipeline(
                        new Pipeline(
                                new Producer(() -> "hello world"),
                                new TransformerExec(
                                        message -> {
                                            message.setContent(message.getContent() + " !");  // 转换成大写
                                            return message;
                                        }
                                )
                        ),
                        new Pipeline(
                                new Producer(() -> "你好"),
                                new TransformerExec(
                                        message -> {
                                            message.setContent(message.getContent() + " ！");
                                            return message;
                                        }
                                )
                        )
                ),
                new Consumer(message -> System.out.println("Consumed: " + message.getContent()))
        );

        // 启动流处理
        all.start();

        // 模拟运行一段时间
        Thread.sleep(1000);
        all.stop();
    }
}
