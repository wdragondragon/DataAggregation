package com.jdragon.aggregation.core.job;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@EqualsAndHashCode(callSuper = true)
@Data
public class PipelineAbstract extends StreamHandler {

    private final ExecutorService executorService;
    private final StreamHandler[] nodes;  // DAG图的节点

    public PipelineAbstract(final StreamHandler[] nodes) {
        this.executorService = Executors.newCachedThreadPool();
        this.nodes = nodes;
    }

    public void start() throws InterruptedException {
        process();
    }

    @Override
    public void process() throws InterruptedException {

    }

    public void stop() {
        for (StreamHandler handler : this.nodes) {
            handler.stop();
        }
        executorService.shutdownNow();
    }
}
