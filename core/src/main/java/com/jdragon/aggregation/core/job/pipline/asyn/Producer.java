package com.jdragon.aggregation.core.job.pipline.asyn;


import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.transport.record.TerminateRecord;

import java.util.function.Supplier;

public class Producer extends StreamHandler {
    private final Supplier<Record> dataSupplier;

    private volatile boolean running = true;

    public Producer(Supplier<Record> dataSupplier) {
        this.dataSupplier = dataSupplier;
    }


    @Override
    public void process() throws InterruptedException {
        while (running) {
            Record data = dataSupplier.get();
            if (data != null) {
                super.put(data);  // 生产数据并推送到下一个节点
                if (data instanceof TerminateRecord) {
                    break;
                }
            }
        }
    }

    @Override
    public void stop() {
        this.running = false;
        this.getOutputQueue().pushTerminate(TerminateRecord.get());
        super.stop();
    }
}
