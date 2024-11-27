package com.jdragon.aggregation.core.job.pipline.asyn;

import com.jdragon.aggregation.core.job.Message;

public class Consumer extends StreamHandler {
    private final java.util.function.Consumer<Message> consumerFunction;

    public Consumer(java.util.function.Consumer<Message> consumerFunction) {
        this.consumerFunction = consumerFunction;
    }

    @Override
    public void process() throws InterruptedException {
        while (true) {
            Message message = super.take();  // 从前一个节点获取消息
            consumerFunction.accept(message);  // 消费消息
        }
    }
}
