package com.jdragon.aggregation.core.job.pipline.asyn;


import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.transport.channel.memory.MemoryChannel;
import com.jdragon.aggregation.core.transport.record.TerminateRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class MergePipeline extends PipelineAbstract {

    private final AtomicBoolean terminated = new AtomicBoolean(false);

    private final AtomicInteger finishedCount = new AtomicInteger(0);

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
                            int finished = finishedCount.incrementAndGet();
                            if (!isRunning() && finished == getNodes().length && terminated.compareAndSet(false, true)) {
                                log.info("merge end");
                                put(TerminateRecord.get());
                                return;
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
