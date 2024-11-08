package com.jdragon.aggregation.core.job.pipline;

import com.jdragon.aggregation.core.job.Message;

import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

public class StreamProducer extends StreamHandler {
    private final Supplier<String> dataSupplier;

    public StreamProducer(Supplier<String> dataSupplier) {
        this.dataSupplier = dataSupplier;
    }

    @Override
    public void process(BlockingQueue<Message> queue) throws InterruptedException {
        while (true) {
            String data = dataSupplier.get();
            if (data != null) {
                Message message = new Message(data);
                queue.put(message);  // 推送数据到队列
            }
        }
    }
}
