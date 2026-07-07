package com.jdragon.aggregation.datasource.scan;

import java.util.LinkedHashMap;
import java.util.Map;

public interface QueueCheckpointReadable {
    default void restoreState(Map<String, Object> state) {
    }

    default Map<String, Object> snapshotState() {
        return new LinkedHashMap<String, Object>();
    }

    default void commitState(Map<String, Object> state) {
    }

    default String deliverySemantics() {
        return "AT_LEAST_ONCE";
    }
}
