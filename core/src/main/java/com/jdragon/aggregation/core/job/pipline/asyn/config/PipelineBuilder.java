package com.jdragon.aggregation.core.job.pipline.asyn.config;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.job.pipline.asyn.*;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;

public class PipelineBuilder {

    public static StreamHandler build(PipelineConfig config) {
        switch (config.getType()) {
            case "Pipeline":
                return buildPipeline(config);
            case "MergePipeline":
                return buildMergePipeline(config);
            case "BroadcastPipeline":
                return buildBroadcastPipeline(config);
            case "Producer":
                return new Producer(loadReader(config.getConfig()));
            case "Consumer":
                return new Consumer(loadWriter(config.getConfig()));
            default:
                throw new IllegalArgumentException("Unsupported type: " + config.getType());
        }
    }

    public static Pipeline buildPipeline(PipelineConfig config) {
        StreamHandler[] array = config.getSteps().stream()
                .map(PipelineBuilder::build).toArray(StreamHandler[]::new);
        return new Pipeline(config.getName(), array);
    }

    private static MergePipeline buildMergePipeline(PipelineConfig config) {
        return new MergePipeline(config.getName(), config.getSteps().stream()
                .map(PipelineBuilder::build).toArray(StreamHandler[]::new));
    }

    private static BroadcastPipeline buildBroadcastPipeline(PipelineConfig config) {
        return new BroadcastPipeline(config.getName(), config.getSteps().stream()
                .map(PipelineBuilder::build).toArray(StreamHandler[]::new));
    }

    private static Reader.Job loadReader(PluginConfig pluginConfig) {
        try (PluginClassLoaderCloseable cl = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.READER, pluginConfig.getType())) {
            Reader.Job job = cl.loadPlugin();
            Configuration config = Configuration.from(pluginConfig.getConfig());
            job.setPluginJobConf(config);
            job.init();
            return job;
        }
    }

    private static Writer.Job loadWriter(PluginConfig pluginConfig) {
        try (PluginClassLoaderCloseable cl = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(PluginType.WRITER, pluginConfig.getType())) {
            Writer.Job job = cl.loadPlugin();
            job.setPluginJobConf(Configuration.from(pluginConfig.getConfig()));
            job.init();
            return job;
        }
    }
}
