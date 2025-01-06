package com.jdragon.aggregation.core.job;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.concurrent.BlockingQueue;

@EqualsAndHashCode(callSuper = true)
@Data
public abstract class StreamHandler extends AbstractJobPlugin {
    private BlockingQueue<Record> inputQueue;

    private BlockingQueue<Record> outputQueue;

    public StreamHandler() {
    }

    public void put(final Record message) throws InterruptedException {
        getOutputQueue().put(message);
    }

    public Record take() throws InterruptedException {
        return getInputQueue().take();
    }

    public abstract void process() throws InterruptedException;

    public void stop() {
    }
}
