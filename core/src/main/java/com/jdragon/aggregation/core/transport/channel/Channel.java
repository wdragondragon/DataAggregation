package com.jdragon.aggregation.core.transport.channel;


import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.statistics.communication.Communication;
import com.jdragon.aggregation.core.statistics.communication.CommunicationTool;
import com.jdragon.aggregation.core.transport.record.TerminateRecord;
import lombok.Getter;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;


@Getter
public abstract class Channel {

    private static final Logger LOG = LoggerFactory.getLogger(Channel.class);

    protected int byteCapacity;

    protected volatile boolean isClosed = false;

    protected volatile long waitReaderTime = 0;

    protected volatile long waitWriterTime = 0;

    private Communication communication = new Communication();

    public Channel() {
        this.byteCapacity = 8 * 1024 * 1024;
    }

    public void close() {
        this.isClosed = true;
    }

    public void open() {
        this.isClosed = false;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public void setCommunication(Communication communication) {
        Validate.notNull(communication);
        this.communication = communication;
    }

    public void push(final Record r) {
        Validate.notNull(r, "record不能为空.");
        this.doPush(r);
        this.statPush(1L, r.getByteSize());
    }

    public void pushTerminate(final TerminateRecord r) {
        Validate.notNull(r, "record不能为空.");
        this.doPush(r);
    }

    public void pushAll(final Collection<Record> rs) {
        Validate.notNull(rs);
        Validate.noNullElements(rs);
        this.doPushAll(rs);
        this.statPush(rs.size(), this.getByteSize(rs));
    }

    public Record pull() {
        Record record = this.doPull();
        this.statPull(1L, record.getByteSize());
        return record;
    }

    public void pullAll(final Collection<Record> rs) {
        Validate.notNull(rs);
        this.doPullAll(rs);
        this.statPull(rs.size(), this.getByteSize(rs));
    }

    protected abstract void doPush(Record r);

    protected abstract void doPushAll(Collection<Record> rs);

    protected abstract Record doPull();

    protected abstract void doPullAll(Collection<Record> rs);

    public abstract List<Record> pullAll();

    public abstract int size();

    public abstract boolean isEmpty();

    public abstract void clear();

    private long getByteSize(final Collection<Record> rs) {
        long size = 0;
        for (final Record each : rs) {
            size += each.getByteSize();
        }
        return size;
    }

    private void statPush(long recordSize, long byteSize) {
        communication.increaseCounter(CommunicationTool.READ_SUCCEED_RECORDS,
                recordSize);
        communication.increaseCounter(CommunicationTool.READ_SUCCEED_BYTES,
                byteSize);
        //在读的时候进行统计waitCounter即可，因为写（pull）的时候可能正在阻塞，但读的时候已经能读到这个阻塞的counter数

        communication.setLongCounter(CommunicationTool.WAIT_READER_TIME, waitReaderTime);
        communication.setLongCounter(CommunicationTool.WAIT_WRITER_TIME, waitWriterTime);

    }

    private void statPull(long recordSize, long byteSize) {
        communication.increaseCounter(
                CommunicationTool.WRITE_RECEIVED_RECORDS, recordSize);
        communication.increaseCounter(
                CommunicationTool.WRITE_RECEIVED_BYTES, byteSize);
    }

    public String statPrint() {
        return "\n" + statPrintRead() + "\n" + statPrintWrite();
    }

    public String statPrintRead() {
        Long waitReaderTime = communication.getLongCounter(CommunicationTool.WAIT_READER_TIME);
        Long readSucceedRecords = communication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS);
        Long readSucceedBytes = communication.getLongCounter(CommunicationTool.READ_SUCCEED_BYTES);
        return String.format("reader info: %d records, %d bytes, wait read time %,.3fs",
                readSucceedRecords, readSucceedBytes,
                ((float) TimeUnit.NANOSECONDS.toNanos(waitReaderTime)) / 1000000000);
    }

    public String statPrintWrite() {
        Long waitWriterTime = communication.getLongCounter(CommunicationTool.WAIT_WRITER_TIME);
        Long writeReceivedRecords = communication.getLongCounter(CommunicationTool.WRITE_RECEIVED_RECORDS);
        Long writeReceivedBytes = communication.getLongCounter(CommunicationTool.WRITE_RECEIVED_BYTES);
        return String.format("writer info: %d records, %d bytes, wait write time %,.3fs",
                writeReceivedRecords, writeReceivedBytes,
                ((float) TimeUnit.NANOSECONDS.toNanos(waitWriterTime)) / 1000000000);
    }
}