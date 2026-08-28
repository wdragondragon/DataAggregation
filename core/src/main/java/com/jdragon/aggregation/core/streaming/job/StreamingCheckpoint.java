package com.jdragon.aggregation.core.streaming.job;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class StreamingCheckpoint {

    private final String batchId;
    private final Map<String, Object> state;

    public StreamingCheckpoint(String batchId, Map<String, Object> state) {
        if (batchId == null || batchId.trim().isEmpty()) {
            throw new IllegalArgumentException("streaming checkpoint batchId must not be blank");
        }
        this.batchId = batchId.trim();
        this.state = Collections.unmodifiableMap(new LinkedHashMap<String, Object>(
                state == null ? Collections.<String, Object>emptyMap() : state));
    }

    public String getBatchId() {
        return batchId;
    }

    public Map<String, Object> getState() {
        return state;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof StreamingCheckpoint)) {
            return false;
        }
        StreamingCheckpoint that = (StreamingCheckpoint) other;
        return batchId.equals(that.batchId) && state.equals(that.state);
    }

    @Override
    public int hashCode() {
        return Objects.hash(batchId, state);
    }
}
