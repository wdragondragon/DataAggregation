package com.jdragon.aggregation.core.job.pipline.asyn;


import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.transport.channel.memory.MemoryChannel;
import com.jdragon.aggregation.core.transport.exchanger.BufferedRecordExchanger;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Producer extends StreamHandler {

    private final Reader.Job reader;

    public Producer(Reader.Job reader) {
        this.reader = reader;
        this.setOutputQueue(new MemoryChannel());
    }

    @Override
    public void process() throws InterruptedException {
        if (reader != null) {
            BufferedRecordExchanger bufferedRecordExchanger = new BufferedRecordExchanger(getOutputQueue());
            reader.startRead(bufferedRecordExchanger);
            bufferedRecordExchanger.terminate();
            end();
            log.info("writer produce exit run status {}", isRunning());
        }
    }
}
