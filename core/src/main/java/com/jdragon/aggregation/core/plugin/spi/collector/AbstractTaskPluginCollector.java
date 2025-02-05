package com.jdragon.aggregation.core.plugin.spi.collector;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.TaskPluginCollector;
import com.jdragon.aggregation.core.statistics.communication.Communication;
import com.jdragon.aggregation.core.statistics.communication.CommunicationTool;
import com.jdragon.aggregation.core.utils.FrameworkErrorCode;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jingxing on 14-9-11.
 */
@Getter
public abstract class AbstractTaskPluginCollector extends TaskPluginCollector {
    private static final Logger LOG = LoggerFactory
            .getLogger(AbstractTaskPluginCollector.class);

    private final Communication communication;

    private final Configuration configuration;

    private final PluginType pluginType;

    public AbstractTaskPluginCollector(Configuration conf, Communication communication,
                                       PluginType type) {
        this.configuration = conf;
        this.communication = communication;
        this.pluginType = type;
    }

    @Override
    final public void collectMessage(String key, String value) {
        this.communication.addMessage(key, value);
    }

    @Override
    public void collectDirtyRecord(Record dirtyRecord, Throwable t,
                                   String errorMessage) {

        if (null == dirtyRecord) {
            LOG.warn("脏数据record=null.");
            return;
        }

        if (this.pluginType.equals(PluginType.READER)) {
            this.communication.increaseCounter(
                    CommunicationTool.READ_FAILED_RECORDS, 1);
            this.communication.increaseCounter(
                    CommunicationTool.READ_FAILED_BYTES, dirtyRecord.getByteSize());
        } else if (this.pluginType.equals(PluginType.WRITER)) {
            this.communication.increaseCounter(
                    CommunicationTool.WRITE_FAILED_RECORDS, 1);
            this.communication.increaseCounter(
                    CommunicationTool.WRITE_FAILED_BYTES, dirtyRecord.getByteSize());
        } else {
            throw AggregationException.asException(
                    FrameworkErrorCode.RUNTIME_ERROR,
                    String.format("不知道的插件类型[%s].", this.pluginType));
        }
    }
}
