package com.jdragon.aggregation.core.streaming.job;

import com.jdragon.aggregation.commons.element.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class StreamingBatch {

    private static final StreamingBatch EMPTY = new StreamingBatch(
            Collections.<Record>emptyList(), null, 0L, 0L);

    private final List<Record> records;
    private final StreamingCheckpoint checkpoint;
    private final long windowStartMillis;
    private final long totalBytes;

    public StreamingBatch(List<Record> records,
                          StreamingCheckpoint checkpoint,
                          long windowStartMillis,
                          long totalBytes) {
        List<Record> safeRecords = records == null
                ? Collections.<Record>emptyList() : new ArrayList<Record>(records);
        if (!safeRecords.isEmpty() && checkpoint == null) {
            throw new IllegalArgumentException("non-empty streaming batch requires a checkpoint");
        }
        if (safeRecords.contains(null)) {
            throw new IllegalArgumentException("streaming batch records must not contain null");
        }
        this.records = Collections.unmodifiableList(safeRecords);
        this.checkpoint = checkpoint;
        this.windowStartMillis = Math.max(0L, windowStartMillis);
        this.totalBytes = totalBytes > 0L ? totalBytes : calculateBytes(safeRecords);
    }

    public static StreamingBatch empty() {
        return EMPTY;
    }

    public boolean isEmpty() {
        return checkpoint == null;
    }

    public List<Record> getRecords() {
        return records;
    }

    public StreamingCheckpoint getCheckpoint() {
        return checkpoint;
    }

    public String getBatchId() {
        return checkpoint == null ? null : checkpoint.getBatchId();
    }

    public long getWindowStartMillis() {
        return windowStartMillis;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    private long calculateBytes(List<Record> values) {
        long result = 0L;
        for (Record record : values) {
            result += Math.max(0, record.getByteSize());
        }
        return result;
    }
}
