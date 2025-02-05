package com.jdragon.aggregation.core.statistics.communication;

import com.jdragon.aggregation.commons.statistics.PerfTrace;
import com.jdragon.aggregation.commons.util.StrUtil;
import org.apache.commons.lang.Validate;

import java.text.DecimalFormat;

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
    private static final String TOTAL_READ_BYTES = "totalReadBytes";

    private static final String TOTAL_ERROR_RECORDS = "totalErrorRecords";
    private static final String TOTAL_ERROR_BYTES = "totalErrorBytes";

    public static final String TOTAL_DIRTY_RECORDS = "totalDirtyRecords";

    private static final String WRITE_SUCCEED_RECORDS = "writeSucceedRecords";
    private static final String WRITE_SUCCEED_BYTES = "writeSucceedBytes";


    public static final String BYTE_SPEED = "byteSpeed";
    public static final String RECORD_SPEED = "recordSpeed";

    public static final String STAGE = "stage";

    public static final String TRANSFORMER_USED_TIME = "totalTransformerUsedTime";
    public static final String TRANSFORMER_SUCCEED_RECORDS = "totalTransformerSuccessRecords";
    public static final String TRANSFORMER_FAILED_RECORDS = "totalTransformerFailedRecords";
    public static final String TRANSFORMER_FILTER_RECORDS = "totalTransformerFilterRecords";

    public static class Stringify {
        private final static DecimalFormat df = new DecimalFormat("0.00");

        public static String getSnapshot(final Communication communication) {
            StringBuilder sb = new StringBuilder();
            sb.append("\n\tTotal ");
            sb.append(getTotal(communication));
            sb.append(" | ");
            sb.append("Speed ");
            sb.append(getSpeed(communication));
            sb.append(" | ");
            sb.append("Error ");
            sb.append(getError(communication));
            sb.append("\n\tAll Task WaitWriterTime ");
            sb.append(PerfTrace.unitTime(communication.getLongCounter(WAIT_WRITER_TIME)));
            sb.append(" | ");
            sb.append(" All Task WaitReaderTime ");
            sb.append(PerfTrace.unitTime(communication.getLongCounter(WAIT_READER_TIME)));
            if (communication.getLongCounter(CommunicationTool.TRANSFORMER_USED_TIME) > 0
                    || communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS) > 0
                    || communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS) > 0
                    || communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS) > 0) {
                sb.append("\n\tTransfermor Success ");
                sb.append(String.format("%d records", communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS)));
                sb.append(" | ");
                sb.append("Transformer Error ");
                sb.append(String.format("%d records", communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS)));
                sb.append(" | ");
                sb.append("Transformer Filter ");
                sb.append(String.format("%d records", communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS)));
                sb.append(" | ");
                sb.append("Transformer usedTime ");
                sb.append(PerfTrace.unitTime(communication.getLongCounter(CommunicationTool.TRANSFORMER_USED_TIME)));
            }
            return sb.toString();
        }


        private static String getTotal(final Communication communication) {
            return String.format("%d records, %d bytes",
                    communication.getLongCounter(TOTAL_READ_RECORDS),
                    communication.getLongCounter(TOTAL_READ_BYTES));
        }

        private static String getSpeed(final Communication communication) {
            return String.format("%s/s, %d records/s",
                    StrUtil.stringify(communication.getLongCounter(BYTE_SPEED)),
                    communication.getLongCounter(RECORD_SPEED));
        }

        private static String getError(final Communication communication) {
            return String.format("%d records, %d bytes",
                    communication.getLongCounter(TOTAL_ERROR_RECORDS),
                    communication.getLongCounter(TOTAL_ERROR_BYTES));
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
