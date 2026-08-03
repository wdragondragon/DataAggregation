package com.jdragon.aggregation.core.job;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.util.MdcTaskDecorator;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeSession;

import java.util.concurrent.LinkedBlockingQueue;

public class MergePipeline extends PipelineAbstract {

    public MergePipeline(StreamHandler... nodes) {
        super(nodes);
        setOutputQueue(new LinkedBlockingQueue<>());
    }

    @Override
    public void process() throws InterruptedException {
        PluginRuntimeSession runtimeSession = PluginRuntimeSession.current();
        for (StreamHandler node : this.getNodes()) {
            node.process();
            Runnable task = MdcTaskDecorator.wrap(() -> {
                try {
                    while (true) {
                        Record take = node.getOutputQueue().take();
                        put(take);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            getExecutorService().submit(runtimeSession == null ? task : runtimeSession.bind(task));
        }
    }
}
