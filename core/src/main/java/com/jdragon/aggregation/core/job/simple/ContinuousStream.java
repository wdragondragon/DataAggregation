package com.jdragon.aggregation.core.job.simple;

import com.jdragon.aggregation.core.job.Message;

import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ContinuousStream {
    private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final List<Transformer> transformers = new CopyOnWriteArrayList<>();
    private final List<Consumer<Message>> consumers = new CopyOnWriteArrayList<>();

    // 启动源端推送线程
    public void startProducer(Supplier<String> dataSupplier) {
        executor.submit(() -> {
            try {
                while (true) {
                    String data = dataSupplier.get();
                    if (data != null) {
                        Message message = new Message(data);
                        // 按顺序应用 transformers
                        for (Transformer transformer : transformers) {
                            message = transformer.transform(message);
                        }
                        queue.put(message);  // 推送数据到队列
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    // 启动消费者线程
    public void startConsumer() {
        executor.submit(() -> {
            try {
                while (true) {
                    Message message = queue.take();  // 获取消息
                    for (Consumer<Message> consumer : consumers) {
                        consumer.accept(message);  // 按顺序消费
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    // 注册Transformer
    public void registerTransformer(Transformer transformer) {
        transformers.add(transformer);
    }

    // 注册Consumer
    public void registerConsumer(Consumer<Message> consumer) {
        consumers.add(consumer);
    }

    // 停止流
    public void stop() {
        executor.shutdownNow();
    }
}
