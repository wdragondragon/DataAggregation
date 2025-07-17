package com.jdragon.aggregation.core.job.pipline.asyn;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.transport.channel.Channel;
import lombok.Data;

import java.util.concurrent.atomic.AtomicBoolean;


@Data
public abstract class StreamHandler {

    private Channel inputQueue;

    private Channel outputQueue;

    private AtomicBoolean runStatus = new AtomicBoolean(true);

    public StreamHandler() {
    }

    public void put(final Record message) throws InterruptedException {
        getOutputQueue().push(message);
    }

    public Record take() throws InterruptedException {
        return getInputQueue().pull();
    }

    public abstract void process() throws InterruptedException;

    public void stop() {
    }

    public void end() {
        runStatus.set(false);
    }

    public boolean isRunning() {
        return runStatus.get();
    }
}
