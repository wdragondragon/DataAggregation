package com.jdragon.aggregation.core.job.pipline;

import com.jdragon.aggregation.core.job.Message;

import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class StreamConsumer extends StreamHandler {
    private final Consumer<Message> consumerFunction;

    public StreamConsumer(Consumer<Message> consumerFunction) {
        this.consumerFunction = consumerFunction;
    }

    @Override
    public void process(BlockingQueue<Message> queue) throws InterruptedException {
        while (true) {
            Message message = queue.take();  // 从队列中获取消息
            consumerFunction.accept(message);  // 消费消息
        }
    }
}
