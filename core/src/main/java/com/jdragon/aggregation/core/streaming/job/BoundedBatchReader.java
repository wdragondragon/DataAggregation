package com.jdragon.aggregation.core.streaming.job;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;

import java.util.List;

final class BoundedBatchReader extends Reader.Job {

    private final List<Record> records;

    BoundedBatchReader(List<Record> records) {
        this.records = records;
    }

    @Override
    public void startRead(RecordSender recordSender) {
        for (Record record : records) {
            if (Thread.currentThread().isInterrupted()) {
                throw new StreamingExecutionException("bounded streaming batch was interrupted");
            }
            recordSender.sendToWriter(record);
        }
    }
}
