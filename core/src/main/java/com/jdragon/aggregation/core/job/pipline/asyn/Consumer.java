package com.jdragon.aggregation.core.job.pipline.asyn;


import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.core.transport.exchanger.BufferedRecordExchanger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Consumer extends StreamHandler {

    private final Writer.Job writerJob;

    public Consumer(Writer.Job writeJob) {
        this.writerJob = writeJob;
    }

    @Override
    public void process() throws InterruptedException {
        writerJob.startWrite(new BufferedRecordExchanger(getInputQueue()));
        end();
        log.info("consumer close run status: {}", isRunning());
    }
}
