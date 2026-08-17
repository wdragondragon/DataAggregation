package com.jdragon.aggregation.transfer.model;

import java.time.ZoneId;
import java.util.Map;

public record TransferSpec(int schemaVersion,
                           TransferEndpoint source,
                           TransferEndpoint target,
                           TransferSelection selection,
                           TransferMapping mapping,
                           TransferPolicy policy,
                           TransferRuntimeOptions runtime,
                           String timeZone,
                           Map<String, String> parameters) {

    public TransferSpec {
        if (schemaVersion != 1) {
            throw new IllegalArgumentException("unsupported schemaVersion: " + schemaVersion);
        }
        if (source == null || target == null || selection == null || mapping == null) {
            throw new IllegalArgumentException("source, target, selection and mapping are required");
        }
        policy = policy == null ? TransferPolicy.defaults() : policy;
        runtime = runtime == null ? TransferRuntimeOptions.defaults() : runtime;
        timeZone = timeZone == null || timeZone.isBlank() ? "UTC" : timeZone;
        ZoneId.of(timeZone);
        parameters = parameters == null ? Map.of() : Map.copyOf(parameters);
    }
}
