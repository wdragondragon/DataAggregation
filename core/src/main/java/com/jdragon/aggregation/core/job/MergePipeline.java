package com.jdragon.aggregation.core.job;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.util.MdcTaskDecorator;

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
            getExecutorService().submit(MdcTaskDecorator.wrap(() -> {
                try {
                    while (true) {
                        Record take = node.getOutputQueue().take();
                        put(take);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
        }
    }
}
