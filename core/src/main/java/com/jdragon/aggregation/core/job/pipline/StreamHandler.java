package com.jdragon.aggregation.core.job.pipline;

import com.jdragon.aggregation.core.job.Message;

import java.util.concurrent.BlockingQueue;

public abstract class StreamHandler {
    public abstract void process(BlockingQueue<Message> queue) throws InterruptedException;
}
