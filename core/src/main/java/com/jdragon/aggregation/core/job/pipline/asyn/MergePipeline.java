package com.jdragon.aggregation.core.job.pipline.asyn;


import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.transport.channel.memory.MemoryChannel;
import com.jdragon.aggregation.core.transport.exchanger.BufferedRecordExchanger;

public class MergePipeline extends PipelineAbstract {

    public MergePipeline(StreamHandler... nodes) {
        super(nodes);
        setOutputQueue(new MemoryChannel());
    }

    @Override
    public void process() throws InterruptedException {
        for (StreamHandler node : this.getNodes()) {
            node.process();
            getExecutorService().submit(() -> {
                try {
                    while (true) {
                        Record record = node.getOutputQueue().pull();
                        put(record);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }
}
