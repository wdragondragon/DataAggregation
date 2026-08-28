package com.jdragon.aggregation.core.streaming.job;

import com.jdragon.aggregation.commons.util.Configuration;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class StreamingSourceContext {

    private final Configuration readerConfiguration;
    private final Configuration writerConfiguration;
    private final Map<String, Object> runContext;

    public StreamingSourceContext(Configuration readerConfiguration,
                                  Configuration writerConfiguration,
                                  Map<String, Object> runContext) {
        this.readerConfiguration = readerConfiguration == null
                ? Configuration.newDefault() : readerConfiguration.clone();
        this.writerConfiguration = writerConfiguration == null
                ? Configuration.newDefault() : writerConfiguration.clone();
        this.runContext = Collections.unmodifiableMap(new LinkedHashMap<String, Object>(
                runContext == null ? Collections.<String, Object>emptyMap() : runContext));
    }

    public Configuration getReaderConfiguration() {
        return readerConfiguration.clone();
    }

    public Configuration getWriterConfiguration() {
        return writerConfiguration.clone();
    }

    public Map<String, Object> getRunContext() {
        return runContext;
    }
}
