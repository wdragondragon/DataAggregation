package com.jdragon.aggregation.transfer.model;

import java.util.Map;

public record TransferEndpoint(String plugin,
                               String identity,
                               Map<String, Object> config) {

    public TransferEndpoint {
        if (plugin == null || plugin.isBlank()) {
            throw new IllegalArgumentException("endpoint plugin is required");
        }
        identity = identity == null || identity.isBlank() ? plugin : identity;
        config = config == null ? Map.of() : Map.copyOf(config);
    }
}
