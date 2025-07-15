package com.jdragon.aggregation.core.job.pipline.asyn;


import com.jdragon.aggregation.core.transport.channel.Channel;
import com.jdragon.aggregation.core.transport.channel.memory.MemoryChannel;
import com.jdragon.aggregation.core.transport.exchanger.BufferedRecordExchanger;

import java.util.concurrent.*;

public class Pipeline extends PipelineAbstract {
    public Pipeline(StreamHandler... nodes) {
        super(nodes);
        StreamHandler pre = null;
        for (StreamHandler node : this.getNodes()) {
            if (node.getOutputQueue() == null) {
                node.setOutputQueue(new MemoryChannel());
            }
            if (pre != null) {
                node.setInputQueue(pre.getOutputQueue());
            }
            pre = node;
        }
    }

    // 启动流管道，依次处理所有流处理器
    @Override
    public void process() throws InterruptedException {
        for (StreamHandler node : getNodes()) {
            getExecutorService().submit(() -> {
                try {
                    node.process();  // 启动流处理器任务
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    @Override
    public void setInputQueue(Channel inputQueue) {
        getNodes()[0].setInputQueue(inputQueue);
    }

    @Override
    public void setOutputQueue(Channel outputQueue) {
        getNodes()[getNodes().length - 1].setOutputQueue(outputQueue);
    }

    @Override
    public Channel getInputQueue() {
        return getNodes()[0].getInputQueue();
    }

    @Override
    public Channel getOutputQueue() {
        return getNodes()[getNodes().length - 1].getOutputQueue();
    }
}
