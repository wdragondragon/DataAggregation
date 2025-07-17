package com.jdragon.aggregation.core.job.pipline.asyn.config;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;

@Data
public class PluginConfig {
    private String type;
    private Map<String, Object> config = new HashMap<>();
}
