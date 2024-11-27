package com.jdragon.aggregation.core.job.pipline.asyn;

import com.jdragon.aggregation.core.job.Message;

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
                        Message take = node.getOutputQueue().take();
                        put(take);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }
}
