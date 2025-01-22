package com.jdragon.aggregation.core.transport.exchanger;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.exception.CommonErrorCode;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.transport.channel.Channel;
import com.jdragon.aggregation.core.transport.record.TerminateRecord;
import com.jdragon.aggregation.core.utils.FrameworkErrorCode;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class BufferedRecordExchanger implements RecordSender, RecordReceiver {

    private final Channel channel;

    private final List<Record> buffer;

    private int bufferSize;

    protected final int byteCapacity;

    private final AtomicInteger memoryBytes = new AtomicInteger(0);

    private int bufferIndex = 0;

    private static Class<? extends Record> RECORD_CLASS;

    private volatile boolean shutdown = false;

    @SuppressWarnings("unchecked")
    public BufferedRecordExchanger(final Channel channel) {
        assert null != channel;
        this.channel = channel;

        this.bufferSize = 1024;
        this.buffer = new ArrayList<>(bufferSize);

        //channel的queue默认大小为8M，原来为64M
        this.byteCapacity = 8 * 1024 * 1024;

        try {
            BufferedRecordExchanger.RECORD_CLASS = ((Class<? extends Record>) Class
                    .forName("com.jdragon.aggregation.core.transport.record.DefaultRecord"));
        } catch (Exception e) {
            throw AggregationException.asException(
                    FrameworkErrorCode.CONFIG_ERROR, e);
        }
    }

    @Override
    public Record createRecord() {
        try {
            return BufferedRecordExchanger.RECORD_CLASS.newInstance();
        } catch (Exception e) {
            throw AggregationException.asException(
                    FrameworkErrorCode.CONFIG_ERROR, e);
        }
    }

    @Override
    public void sendToWriter(Record record) {
        if (shutdown) {
            throw AggregationException.asException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }
        Validate.notNull(record, "record不能为空.");

        boolean isFull = (this.bufferIndex >= this.bufferSize || this.memoryBytes.get() + record.getMemorySize() > this.byteCapacity);
        if (isFull) {
            flush();
        }

        this.buffer.add(record);
        this.bufferIndex++;
        memoryBytes.addAndGet(record.getMemorySize());
    }

    @Override
    public void flush() {
        if (shutdown) {
            throw AggregationException.asException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }
        this.channel.pushAll(this.buffer);
        this.buffer.clear();
        this.bufferIndex = 0;
        this.memoryBytes.set(0);
    }

    @Override
    public void terminate() {
        if (shutdown) {
            throw AggregationException.asException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }
        flush();
        this.channel.pushTerminate(TerminateRecord.get());
    }

    @Override
    public Record getFromReader() {
        if (shutdown) {
            throw AggregationException.asException(CommonErrorCode.SHUT_DOWN_TASK, "");
        }
        boolean isEmpty = (this.bufferIndex >= this.buffer.size());
        if (isEmpty) {
            receive();
        }

        Record record = this.buffer.get(this.bufferIndex++);
        if (record instanceof TerminateRecord) {
            record = null;
        }
        return record;
    }

    @Override
    public void shutdown() {
        shutdown = true;
        try {
            buffer.clear();
            channel.clear();
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private void receive() {
        this.channel.pullAll(this.buffer);
        this.bufferIndex = 0;
        this.bufferSize = this.buffer.size();
    }
}
