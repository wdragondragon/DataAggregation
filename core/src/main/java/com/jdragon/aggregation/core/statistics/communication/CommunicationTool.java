package com.jdragon.aggregation.core.statistics.communication;

import org.apache.commons.lang.Validate;

public class CommunicationTool {
    public static final String READ_SUCCEED_RECORDS = "readSucceedRecords";
    public static final String READ_SUCCEED_BYTES = "readSucceedBytes";

    public static final String READ_FAILED_RECORDS = "readFailedRecords";
    public static final String READ_FAILED_BYTES = "readFailedBytes";
    public static final String WRITE_FAILED_RECORDS = "writeFailedRecords";
    public static final String WRITE_FAILED_BYTES = "writeFailedBytes";


    public static final String WAIT_WRITER_TIME = "waitWriterTime";

    public static final String WAIT_READER_TIME = "waitReaderTime";

    public static final String WRITE_RECEIVED_RECORDS = "writeReceivedRecords";
    public static final String WRITE_RECEIVED_BYTES = "writeReceivedBytes";

    public static final String TOTAL_READ_RECORDS = "totalReadRecords";
    public static final String TOTAL_READ_BYTES = "totalReadBytes";

    public static final String TOTAL_ERROR_RECORDS = "totalErrorRecords";
    public static final String TOTAL_ERROR_BYTES = "totalErrorBytes";

    public static final String TOTAL_DIRTY_RECORDS = "totalDirtyRecords";

    public static final String WRITE_SUCCEED_RECORDS = "writeSucceedRecords";
    public static final String WRITE_SUCCEED_BYTES = "writeSucceedBytes";


    public static final String BYTE_SPEED = "byteSpeed";
    public static final String RECORD_SPEED = "recordSpeed";

    public static final String TRANSFORMER_USED_TIME = "totalTransformerUsedTime";
    public static final String TRANSFORMER_SUCCEED_RECORDS = "totalTransformerSuccessRecords";
    public static final String TRANSFORMER_FAILED_RECORDS = "totalTransformerFailedRecords";
    public static final String TRANSFORMER_FILTER_RECORDS = "totalTransformerFilterRecords";

    public static final String START_TIME = "startTime";
    public static final String END_TIME = "endTime";
    public static final String TIME_INTERVAL_SECONDS = "timeIntervalSeconds";

    public static class Stringify {
        public static String getSnapshot(final Communication communication) {
            RunStatus runStatus = new RunStatus(communication);
            return runStatus.getStringify();
        }
    }


    public static Communication getReportCommunication(Communication now, Communication old) {
        Validate.isTrue(now != null && old != null,
                "为汇报准备的新旧metric不能为null");

        long totalReadRecords = getTotalReadRecords(now);
        long totalReadBytes = getTotalReadBytes(now);
        now.setLongCounter(TOTAL_READ_RECORDS, totalReadRecords);
        now.setLongCounter(TOTAL_READ_BYTES, totalReadBytes);
        now.setLongCounter(TOTAL_ERROR_RECORDS, getTotalErrorRecords(now));
        now.setLongCounter(TOTAL_ERROR_BYTES, getTotalErrorBytes(now));
        now.setLongCounter(WRITE_SUCCEED_RECORDS, getWriteSucceedRecords(now));
        now.setLongCounter(WRITE_SUCCEED_BYTES, getWriteSucceedBytes(now));

        long timeInterval = now.getTimestamp() - old.getTimestamp();
        long sec = timeInterval <= 1000 ? 1 : timeInterval / 1000;
        long bytesSpeed = (totalReadBytes
                - getTotalReadBytes(old)) / sec;
        long recordsSpeed = (totalReadRecords
                - getTotalReadRecords(old)) / sec;

        now.setLongCounter(BYTE_SPEED, bytesSpeed < 0 ? 0 : bytesSpeed);
        now.setLongCounter(RECORD_SPEED, recordsSpeed < 0 ? 0 : recordsSpeed);

        if (old.getThrowable() != null) {
            now.setThrowable(old.getThrowable());
        }
        now.setLongCounter(START_TIME, old.getTimestamp());
        now.setLongCounter(END_TIME, now.getTimestamp());
        now.setLongCounter(TIME_INTERVAL_SECONDS, timeInterval);
        return now;
    }


    public static long getTotalReadRecords(final Communication communication) {
        return communication.getLongCounter(READ_SUCCEED_RECORDS) +
                communication.getLongCounter(READ_FAILED_RECORDS);
    }

    public static long getTotalReadBytes(final Communication communication) {
        return communication.getLongCounter(READ_SUCCEED_BYTES) +
                communication.getLongCounter(READ_FAILED_BYTES);
    }

    public static long getTotalErrorRecords(final Communication communication) {
        return communication.getLongCounter(READ_FAILED_RECORDS) +
                communication.getLongCounter(WRITE_FAILED_RECORDS);
    }

    public static long getTotalErrorBytes(final Communication communication) {
        return communication.getLongCounter(READ_FAILED_BYTES) +
                communication.getLongCounter(WRITE_FAILED_BYTES);
    }

    public static long getWriteSucceedRecords(final Communication communication) {
        return communication.getLongCounter(WRITE_RECEIVED_RECORDS) -
                communication.getLongCounter(WRITE_FAILED_RECORDS);
    }

    public static long getWriteSucceedBytes(final Communication communication) {
        return communication.getLongCounter(WRITE_RECEIVED_BYTES) -
                communication.getLongCounter(WRITE_FAILED_BYTES);
    }
}
