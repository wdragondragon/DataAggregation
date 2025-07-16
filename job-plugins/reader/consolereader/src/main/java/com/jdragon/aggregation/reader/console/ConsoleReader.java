package com.jdragon.aggregation.reader.console;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import org.apache.commons.lang.RandomStringUtils;

public class ConsoleReader extends Reader.Job {

    private volatile boolean running = true;

    private int rowCount;

    @Override
    public void init() {
        Configuration pluginJobConf = getPluginJobConf();
        this.rowCount = pluginJobConf.getInt("rowCount", 1024);
        super.init();
    }

    @Override
    public void startRead(RecordSender recordSender) {
        int count = 0;
        while (running && count < rowCount) {
            Record record = new DefaultRecord();
            record.addColumn(new StringColumn(RandomStringUtils.randomAlphanumeric(10)));
            recordSender.sendToWriter(record);
            count++;
        }
    }

    @Override
    public void destroy() {
        this.running = false;
        super.destroy();
    }
}
