package com.jdragon.aggregation.writer.console;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConsoleWriter extends Writer.Job {
    @Override
    public void startWrite(RecordReceiver recordReceiver) {
        Record record;
        while ((record = recordReceiver.getFromReader()) != null) {
            log.info(record.toString());
        }
    }
}
