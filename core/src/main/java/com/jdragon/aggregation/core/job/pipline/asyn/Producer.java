package com.jdragon.aggregation.core.job.pipline.asyn;


import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.transport.channel.memory.MemoryChannel;
import com.jdragon.aggregation.core.transport.exchanger.BufferedRecordExchanger;
import com.jdragon.aggregation.core.transport.record.TerminateRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Supplier;

@Slf4j
public class Producer extends StreamHandler {

    private Supplier<Record> dataSupplier;

    private BufferedRecordExchanger bufferedRecordExchanger;

    private Reader.Job reader;

    public Producer(Supplier<Record> dataSupplier) {
        this.dataSupplier = dataSupplier;
    }

    public Producer(Reader.Job reader) {
        this.reader = reader;
        this.setOutputQueue(new MemoryChannel());
    }

    @Override
    public void process() throws InterruptedException {
        if (reader != null) {
            bufferedRecordExchanger = new BufferedRecordExchanger(getOutputQueue());
            reader.startRead(bufferedRecordExchanger);
            bufferedRecordExchanger.terminate();
            end();
            log.info("writer produce exit run status {}", isRunning());
        } else {
            while (isRunning()) {
                Record data = dataSupplier.get();
                super.put(data);  // 生产数据并推送到下一个节点
                if (data == null || data instanceof TerminateRecord) {
                    end();
                    break;
                }
            }
            log.info("lambda produce exit run status {}", isRunning());
        }
    }

    @Override
    public void stop() {
        getRunStatus().set(false);
        super.stop();
    }
}
