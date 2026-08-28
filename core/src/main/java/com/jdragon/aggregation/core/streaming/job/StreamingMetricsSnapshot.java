package com.jdragon.aggregation.core.streaming.job;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class StreamingMetricsSnapshot {

    private static final StreamingMetricsSnapshot EMPTY = new StreamingMetricsSnapshot(
            Collections.<String, Long>emptyMap(), Collections.<String, Long>emptyMap(), null);

    private final Map<String, Long> counters;
    private final Map<String, Long> gauges;
    private final Long lastMessageTimestamp;

    public StreamingMetricsSnapshot(Map<String, Long> counters,
                                    Map<String, Long> gauges,
                                    Long lastMessageTimestamp) {
        this.counters = immutableCopy(counters);
        this.gauges = immutableCopy(gauges);
        this.lastMessageTimestamp = lastMessageTimestamp;
    }

    public static StreamingMetricsSnapshot empty() {
        return EMPTY;
    }

    public Map<String, Long> getCounters() {
        return counters;
    }

    public Map<String, Long> getGauges() {
        return gauges;
    }

    public Long getLastMessageTimestamp() {
        return lastMessageTimestamp;
    }

    private Map<String, Long> immutableCopy(Map<String, Long> values) {
        return Collections.unmodifiableMap(new LinkedHashMap<String, Long>(
                values == null ? Collections.<String, Long>emptyMap() : values));
    }
}
