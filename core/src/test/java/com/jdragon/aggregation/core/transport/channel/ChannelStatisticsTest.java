package com.jdragon.aggregation.core.transport.channel;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.core.statistics.communication.Communication;
import com.jdragon.aggregation.core.statistics.communication.CommunicationTool;
import com.jdragon.aggregation.core.transport.channel.memory.MemoryChannel;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.core.transport.record.TerminateRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ChannelStatisticsTest {

    @Test
    public void pullShouldNotCountTerminateRecordAsWriterReceivedRecord() {
        Communication communication = new Communication();
        MemoryChannel channel = new MemoryChannel(16, 16);
        channel.setCommunication(communication);
        Record record = record("customer-001");

        channel.push(record);
        channel.pushTerminate(TerminateRecord.get());

        assertSame(record, channel.pull());
        assertTrue(channel.pull() instanceof TerminateRecord);
        assertEquals(1L, communication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS).longValue());
        assertEquals(1L, communication.getLongCounter(CommunicationTool.WRITE_RECEIVED_RECORDS).longValue());
        assertEquals(1L, CommunicationTool.getWriteSucceedRecords(communication));
    }

    @Test
    public void pullAllShouldNotCountTerminateRecordAsWriterReceivedRecord() {
        Communication communication = new Communication();
        MemoryChannel channel = new MemoryChannel(16, 16);
        channel.setCommunication(communication);
        Record first = record("customer-001");
        Record second = record("customer-002");

        channel.push(first);
        channel.push(second);
        channel.pushTerminate(TerminateRecord.get());

        List<Record> records = new ArrayList<Record>();
        channel.pullAll(records);

        assertEquals(3, records.size());
        assertSame(first, records.get(0));
        assertSame(second, records.get(1));
        assertTrue(records.get(2) instanceof TerminateRecord);
        assertEquals(2L, communication.getLongCounter(CommunicationTool.READ_SUCCEED_RECORDS).longValue());
        assertEquals(2L, communication.getLongCounter(CommunicationTool.WRITE_RECEIVED_RECORDS).longValue());
        assertEquals(2L, CommunicationTool.getWriteSucceedRecords(communication));
    }

    private Record record(String value) {
        DefaultRecord record = new DefaultRecord();
        record.addColumn(new StringColumn(value));
        return record;
    }
}
