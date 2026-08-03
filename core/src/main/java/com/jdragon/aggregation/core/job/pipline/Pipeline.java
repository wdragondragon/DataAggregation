package com.jdragon.aggregation.core.job.pipline;

import com.jdragon.aggregation.core.job.Message;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeSession;

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
        PluginRuntimeSession runtimeSession = PluginRuntimeSession.current();
        for (StreamHandler handler : handlers) {
            Runnable task = () -> {
                try {
                    handler.process(queue);  // 处理流
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            };
            executor.submit(runtimeSession == null ? task : runtimeSession.bind(task));
        }
    }

    public void stop() {
        executor.shutdownNow();
    }
}
