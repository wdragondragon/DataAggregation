package com.jdragon.aggregation.core.job.pipline.asyn.config;

import lombok.Data;

import java.util.List;

@Data
public class PipelineConfig {
    private String type; // Pipeline / MergePipeline / BroadcastPipeline / Producer / Consumer
    private String name;
    private List<PipelineConfig> steps;
    private PluginConfig config;
}