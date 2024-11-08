package com.jdragon.aggregation.core.job.pipline;

import com.jdragon.aggregation.core.job.Message;

import java.util.List;
import java.util.concurrent.*;

public class Pipeline {
    private final BlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private final ExecutorService executor = Executors.newCachedThreadPool();  // 支持并发的线程池
    private final List<StreamHandler> handlers;

    public Pipeline(List<StreamHandler> handlers) {
        this.handlers = handlers;
    }

    // 启动流管道，依次处理所有流处理器
    public void start() {
        for (StreamHandler handler : handlers) {
            executor.submit(() -> {
                try {
                    handler.process(queue);  // 处理流
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    public void stop() {
        executor.shutdownNow();
    }
}
