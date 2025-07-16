package com.jdragon.aggregation.core.job.pipline.asyn;


import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.transport.channel.memory.MemoryChannel;
import com.jdragon.aggregation.core.transport.record.TerminateRecord;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MergePipeline extends PipelineAbstract {

    public MergePipeline(String name, StreamHandler... nodes) {
        super(name, nodes);
        setOutputQueue(new MemoryChannel());
    }

    @Override
    public void process() throws InterruptedException {
        for (StreamHandler node : this.getNodes()) {
            getExecutorService().submit(() -> {
                try {
                    node.process();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            getExecutorService().submit(() -> {
                try {
                    while (true) {
                        Record record = node.getOutputQueue().pull();
                        if (record == null || record instanceof TerminateRecord) {
                            log.info("merge pipeline have node is close, merge status [{}]", isRunning());
                            if (!isRunning()) {
                                log.info("merge end");
                                put(TerminateRecord.get());
                            }
                        } else {
                            put(record);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }
}
