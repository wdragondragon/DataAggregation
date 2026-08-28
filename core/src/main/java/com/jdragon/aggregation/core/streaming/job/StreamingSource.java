package com.jdragon.aggregation.core.streaming.job;

/**
 * Optional capability implemented by readers that can keep a source session
 * alive across multiple bounded writer executions.
 */
public interface StreamingSource {

    StreamingSession open(StreamingSourceContext context) throws Exception;
}
