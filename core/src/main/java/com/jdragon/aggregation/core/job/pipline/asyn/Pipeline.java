package com.jdragon.aggregation.core.job.pipline.asyn;

import com.jdragon.aggregation.core.job.Message;

import java.util.List;
import java.util.concurrent.*;

public class Pipeline extends StreamHandler {
    private final ExecutorService executorService;
    private final StreamHandler[] nodes;  // DAG图的节点

    public Pipeline(StreamHandler... nodes) {
        this.nodes = nodes;
        StreamHandler pre = null;
        for (StreamHandler streamHandler : this.nodes) {
            if (streamHandler.getOutputQueue() == null) {
                streamHandler.setOutputQueue(new LinkedBlockingQueue<>());
            }
            if (pre != null) {
                streamHandler.setInputQueue(pre.getOutputQueue());
            }
            pre = streamHandler;
        }
        this.executorService = Executors.newCachedThreadPool();
    }

    // 启动流管道，依次处理所有流处理器
    @Override
    public void process() {
        for (StreamHandler node : nodes) {
            executorService.submit(() -> {
                try {
                    node.process();  // 启动流处理器任务
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    @Override
    public void setInputQueue(BlockingQueue<Message> inputQueue) {
        nodes[0].setInputQueue(inputQueue);
    }

    @Override
    public void setOutputQueue(BlockingQueue<Message> outputQueue) {
        nodes[nodes.length - 1].setOutputQueue(outputQueue);
    }

    @Override
    public BlockingQueue<Message> getInputQueue() {
        return nodes[0].getInputQueue();
    }

    @Override
    public BlockingQueue<Message> getOutputQueue() {
        return nodes[nodes.length - 1].getOutputQueue();
    }

    public void start() {
        process();
    }

    public void stop() {
        for (StreamHandler handler : this.nodes) {
            handler.stop();
        }
        executorService.shutdownNow();
    }
}
