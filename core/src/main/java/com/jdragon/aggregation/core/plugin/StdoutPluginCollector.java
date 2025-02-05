package com.jdragon.aggregation.core.plugin;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.spi.collector.AbstractTaskPluginCollector;
import com.jdragon.aggregation.core.statistics.communication.Communication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StdoutPluginCollector extends AbstractTaskPluginCollector {
    private static final Logger LOG = LoggerFactory
            .getLogger(StdoutPluginCollector.class);

    public StdoutPluginCollector(Configuration conf, Communication communication, PluginType type) {
        super(conf, communication, type);
    }

    @Override
    public void collectDirtyRecord(Record dirtyRecord, Throwable t,
                                   String errorMessage) {
        LOG.info("dirty record {}", dirtyRecord);
        super.collectDirtyRecord(dirtyRecord, t, errorMessage);
    }
}
