package com.jdragon.aggregation.core.statistics.communication;

import com.jdragon.aggregation.commons.statistics.PerfTrace;
import com.jdragon.aggregation.commons.util.StrUtil;
import com.jdragon.aggregation.core.plugin.JobPointReporter;
import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.jdragon.aggregation.core.statistics.communication.CommunicationTool.*;

@Data
public class RunStatus {

    private long total;

    private long bytes;

    private long error;

    private long bytesError;

    private long transformerSuccess;

    private long transformerError;

    private long transformerFilter;

    private long transformerUsedTime;

    private long byteSpeed;

    private long recordSpeed;

    private long waitWriterTime;

    private long waitReaderTime;

    private Map<String, Object> otherReportInfo = new LinkedHashMap<>();

    private Communication communication;

    private JobPointReporter jobReport;

    public RunStatus(Communication communication) {
        this.communication = communication;
        init();
    }

    public RunStatus(JobPointReporter jobReport) {
        this.jobReport = jobReport;
        this.communication = jobReport.getTrackCommunication();
        init();
    }

    public void init() {
        this.total = communication.getLongCounter(TOTAL_READ_RECORDS);
        this.bytes = communication.getLongCounter(TOTAL_READ_BYTES);
        this.error = communication.getLongCounter(TOTAL_ERROR_RECORDS);
        this.bytesError = communication.getLongCounter(TOTAL_ERROR_BYTES);

        this.byteSpeed = communication.getLongCounter(BYTE_SPEED);
        this.recordSpeed = communication.getLongCounter(RECORD_SPEED);
        this.waitWriterTime = communication.getLongCounter(WAIT_WRITER_TIME);
        this.waitReaderTime = communication.getLongCounter(WAIT_READER_TIME);

        this.transformerUsedTime = communication.getLongCounter(CommunicationTool.TRANSFORMER_USED_TIME);

        this.transformerSuccess = communication.getLongCounter(CommunicationTool.TRANSFORMER_SUCCEED_RECORDS);
        this.transformerError = communication.getLongCounter(CommunicationTool.TRANSFORMER_FAILED_RECORDS);
        this.transformerFilter = communication.getLongCounter(CommunicationTool.TRANSFORMER_FILTER_RECORDS);
    }

    public void put(String key, Object value) {
        otherReportInfo.put(key, value);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) otherReportInfo.get(key);
    }

    public void report() {
        jobReport.report();
    }

    public String getStringify() {
        init();
        RunStatus runStatus = this;
        StringBuilder sb = new StringBuilder();
        sb.append("\n\tTotal ");
        sb.append(String.format("%d records, %d bytes", runStatus.getTotal(), runStatus.getBytes()));
        sb.append(" | ");
        sb.append("Speed ");
        sb.append(String.format("%s/s, %d records/s",
                StrUtil.stringify(runStatus.getByteSpeed()),
                runStatus.getRecordSpeed()));
        sb.append(" | ");
        sb.append("Error ");
        sb.append(String.format("%d records, %d bytes",
                runStatus.getError(),
                runStatus.getBytesError()));
        sb.append("\n\tAll Task WaitWriterTime ");
        sb.append(PerfTrace.unitTime(runStatus.getWaitWriterTime()));
        sb.append(" | ");
        sb.append(" All Task WaitReaderTime ");
        sb.append(PerfTrace.unitTime(runStatus.getWaitReaderTime()));
        if (runStatus.getTransformerUsedTime() > 0 || runStatus.getTransformerSuccess() > 0
                || runStatus.getTransformerError() > 0 || runStatus.getTransformerFilter() > 0) {
            sb.append("\n\tTransfermor Success ");
            sb.append(String.format("%d records", runStatus.getTransformerSuccess()));
            sb.append(" | ");
            sb.append("Transformer Error ");
            sb.append(String.format("%d records", runStatus.getTransformerError()));
            sb.append(" | ");
            sb.append("Transformer Filter ");
            sb.append(String.format("%d records", runStatus.getTransformerFilter()));
            sb.append(" | ");
            sb.append("Transformer usedTime ");
            sb.append(PerfTrace.unitTime(runStatus.getTransformerUsedTime()));
        }
        return sb.toString();
    }
}
