package com.jdragon.aggregation.core.job.pipline.asyn;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.transport.channel.Channel;
import com.jdragon.aggregation.core.transport.exchanger.BufferedRecordExchanger;
import lombok.Data;


@Data
public abstract class StreamHandler {

    private Channel inputQueue;

    private Channel outputQueue;

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
}
