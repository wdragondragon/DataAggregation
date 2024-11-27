package com.jdragon.aggregation.core.job.pipline.asyn;

import com.jdragon.aggregation.core.job.Message;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class MergePipeline extends StreamHandler {
    private final ExecutorService executorService;
    private final StreamHandler[] nodes;

    public MergePipeline(StreamHandler... nodes) {
        setOutputQueue(new LinkedBlockingQueue<>());
        this.nodes = nodes;
        this.executorService = Executors.newCachedThreadPool();
    }

    @Override
    public void process() throws InterruptedException {
        for (StreamHandler node : this.nodes) {
            node.process();
            executorService.submit(() -> {
                try {
                    while (true) {
                        Message take = node.getOutputQueue().take();
                        put(take);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    public void stop() {
        for (StreamHandler handler : this.nodes) {
            handler.stop();
        }
        executorService.shutdownNow();
    }
}
