package com.jdragon.aggregation.core.job.pipline.asyn;


import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.transport.channel.memory.MemoryChannel;
import com.jdragon.aggregation.core.transport.record.TerminateRecord;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeSession;
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
        PluginRuntimeSession runtimeSession = PluginRuntimeSession.current();
        for (StreamHandler node : this.getNodes()) {
            Runnable processTask = () -> {
                try {
                    node.process();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            };
            getExecutorService().submit(runtimeSession == null ? processTask : runtimeSession.bind(processTask));
            Runnable mergeTask = () -> {
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
            };
            getExecutorService().submit(runtimeSession == null ? mergeTask : runtimeSession.bind(mergeTask));
        }
    }
}
