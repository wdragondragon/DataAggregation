package com.jdragon.aggregation.core.job.pipline.asyn;

import com.jdragon.aggregation.core.job.Message;
import lombok.Data;
import lombok.Getter;

import java.util.concurrent.BlockingQueue;


@Data
public abstract class StreamHandler {

    private BlockingQueue<Message> inputQueue;

    private BlockingQueue<Message> outputQueue;

    public StreamHandler() {
    }

    public void put(final Message message) throws InterruptedException {
        getOutputQueue().put(message);
    }

    public Message take() throws InterruptedException {
        return getInputQueue().take();
    }

    public abstract void process() throws InterruptedException;

    public void stop() {
    }
}
