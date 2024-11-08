package com.jdragon.aggregation.core.job.pipline;

import com.jdragon.aggregation.core.job.Message;

import java.util.concurrent.BlockingQueue;

public class Transformer extends StreamHandler {
    private final TransformerFunction function;

    public Transformer(TransformerFunction function) {
        this.function = function;
    }

    @Override
    public void process(BlockingQueue<Message> queue) throws InterruptedException {
        while (true) {
            Message message = queue.take();  // 从队列中获取消息
            Message transformedMessage = function.apply(message);  // 转换消息
            queue.put(transformedMessage);  // 放回队列
        }
    }

    @FunctionalInterface
    public interface TransformerFunction {
        Message apply(Message message);
    }
}
