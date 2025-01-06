package com.jdragon.aggregation.core.job;

import com.jdragon.aggregation.commons.element.Record;

import java.util.concurrent.LinkedBlockingQueue;

public class MergePipeline extends PipelineAbstract {

    public MergePipeline(StreamHandler... nodes) {
        super(nodes);
        setOutputQueue(new LinkedBlockingQueue<>());
    }

    @Override
    public void process() throws InterruptedException {
        for (StreamHandler node : this.getNodes()) {
            node.process();
            getExecutorService().submit(() -> {
                try {
                    while (true) {
                        Record take = node.getOutputQueue().take();
                        put(take);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }
}
