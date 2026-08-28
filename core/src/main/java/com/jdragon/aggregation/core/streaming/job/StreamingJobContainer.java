package com.jdragon.aggregation.core.streaming.job;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.enums.State;
import com.jdragon.aggregation.core.job.JobContainer;
import com.jdragon.aggregation.core.plugin.CustomPluginCreator;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.core.statistics.communication.Communication;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.pluginloader.type.IPluginType;
import lombok.extern.slf4j.Slf4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class StreamingJobContainer {

    private static final int DEFAULT_BATCH_RETRY_COUNT = 3;

    private final Configuration configuration;
    private final StreamingSource source;
    private final Map<IPluginType, CustomPluginCreator> customJobPlugins =
            new LinkedHashMap<IPluginType, CustomPluginCreator>();
    private final List<Transformer> customTransformers = new ArrayList<Transformer>();
    private final Map<String, Object> runContext = new LinkedHashMap<String, Object>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private final AtomicBoolean cancellationRequested = new AtomicBoolean(false);

    private volatile StreamingSession session;
    private volatile JobContainer currentBatchContainer;
    private volatile StreamingCheckpoint lastCommittedCheckpoint;
    private volatile StreamingJobListener listener = StreamingJobListener.NOOP;

    public StreamingJobContainer(Configuration configuration, StreamingSource source) {
        if (configuration == null) {
            throw new IllegalArgumentException("streaming job configuration must not be null");
        }
        if (source == null) {
            throw new IllegalArgumentException("streaming source must not be null");
        }
        this.configuration = configuration.clone();
        this.source = source;
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("streaming job container can only be started once");
        }
        Configuration reader = requiredConfiguration(configuration, "reader");
        Configuration writer = requiredConfiguration(configuration, "writer");
        StreamingSourceContext sourceContext = new StreamingSourceContext(
                reader.getConfiguration("config"), writer.getConfiguration("config"), snapshotRunContext());
        StreamingPollRequest pollRequest = pollRequest();
        try {
            session = source.open(sourceContext);
            if (session == null) {
                throw new StreamingExecutionException("streaming source returned a null session");
            }
            while (!stopRequested.get() && !cancellationRequested.get()) {
                StreamingBatch batch;
                try {
                    batch = session.pollBatch(pollRequest);
                } catch (Exception failure) {
                    if (stopRequested.get() || cancellationRequested.get()) {
                        break;
                    }
                    throw new StreamingExecutionException("streaming source poll failed", failure);
                }
                if (batch == null || batch.isEmpty()) {
                    continue;
                }
                executeAndCommit(batch);
            }
        } catch (StreamingExecutionException failure) {
            if (!stopRequested.get() && !cancellationRequested.get()) {
                throw failure;
            }
        } catch (Exception failure) {
            if (!stopRequested.get() && !cancellationRequested.get()) {
                throw new StreamingExecutionException("streaming job failed", failure);
            }
        } finally {
            closeSession();
            currentBatchContainer = null;
        }
    }

    public void requestStop() {
        stopRequested.set(true);
        StreamingSession active = session;
        if (active != null) {
            active.wakeup();
        }
    }

    public void cancel() {
        if (!cancellationRequested.compareAndSet(false, true)) {
            return;
        }
        stopRequested.set(true);
        JobContainer activeBatch = currentBatchContainer;
        if (activeBatch != null) {
            activeBatch.cancel();
        }
        StreamingSession activeSession = session;
        if (activeSession != null) {
            activeSession.wakeup();
        }
    }

    public void addConsumerPlugin(IPluginType type, CustomPluginCreator customPluginCreator) {
        customJobPlugins.put(type, customPluginCreator);
    }

    public void addConsumerTransformer(Transformer transformer) {
        customTransformers.add(transformer);
    }

    public void setRunContext(String key, Object value) {
        synchronized (runContext) {
            runContext.put(key, value);
        }
    }

    public void setListener(StreamingJobListener listener) {
        this.listener = listener == null ? StreamingJobListener.NOOP : listener;
    }

    public StreamingCheckpoint getLastCommittedCheckpoint() {
        return lastCommittedCheckpoint;
    }

    public StreamingMetricsSnapshot getSourceMetrics() {
        StreamingSession active = session;
        return active == null ? StreamingMetricsSnapshot.empty() : active.metrics();
    }

    public boolean isStopRequested() {
        return stopRequested.get();
    }

    public boolean isCancellationRequested() {
        return cancellationRequested.get();
    }

    private void executeAndCommit(StreamingBatch batch) throws Exception {
        listener.onBatchStarted(batch);
        Throwable lastFailure = null;
        Communication committedCommunication = null;
        int retryCount = Math.max(0, configuration.getInt(
                "streaming.batchRetryCount", DEFAULT_BATCH_RETRY_COUNT));
        int maxAttempts = retryCount + 1;
        long startedAt = System.currentTimeMillis();
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            if (cancellationRequested.get()) {
                StreamingExecutionException cancelled = new StreamingExecutionException(
                        "streaming batch was cancelled before commit");
                abort(batch, cancelled);
                throw cancelled;
            }
            try {
                Communication communication = executeBoundedBatch(batch);
                session.commit(batch.getCheckpoint());
                lastCommittedCheckpoint = batch.getCheckpoint();
                committedCommunication = communication;
                break;
            } catch (Exception failure) {
                lastFailure = failure;
                if (cancellationRequested.get()) {
                    abort(batch, failure);
                    throw new StreamingExecutionException(
                            "streaming batch was cancelled before commit", failure);
                }
            }
            if (attempt <= retryCount) {
                listener.onBatchRetry(batch, attempt, lastFailure);
                sleepBeforeRetry(attempt);
            }
        }
        if (committedCommunication != null) {
            try {
                listener.onBatchCommitted(batch, committedCommunication,
                        System.currentTimeMillis() - startedAt);
            } catch (RuntimeException failure) {
                throw new StreamingExecutionException(
                        "streaming batch " + batch.getBatchId()
                                + " was committed but post-commit listener failed",
                        failure);
            }
            return;
        }
        abort(batch, lastFailure);
        listener.onBatchFailed(batch, lastFailure);
        throw new StreamingExecutionException(
                "streaming batch " + batch.getBatchId() + " failed after " + maxAttempts + " attempts",
                lastFailure);
    }

    private Communication executeBoundedBatch(StreamingBatch batch) {
        Configuration batchConfiguration = resolveBatchVariables(configuration, batch);
        JobContainer batchContainer = new JobContainer(batchConfiguration);
        currentBatchContainer = batchContainer;
        for (Map.Entry<IPluginType, CustomPluginCreator> entry : customJobPlugins.entrySet()) {
            if (entry.getKey() != PluginType.READER) {
                batchContainer.addConsumerPlugin(entry.getKey(), entry.getValue());
            }
        }
        for (Transformer transformer : customTransformers) {
            batchContainer.addConsumerTransformer(transformer);
        }
        List<Record> attemptRecords = copyRecords(batch.getRecords());
        batchContainer.addConsumerPlugin(PluginType.READER,
                (pluginConfiguration, peerConfiguration) -> new BoundedBatchReader(attemptRecords));
        Map<String, Object> batchContext = snapshotRunContext();
        batchContext.put("stream.batchId", batch.getBatchId());
        batchContext.put("stream.windowStart", Instant.ofEpochMilli(batch.getWindowStartMillis()).toString());
        for (Map.Entry<String, Object> entry : batchContext.entrySet()) {
            batchContainer.setRunContext(entry.getKey(), entry.getValue());
        }
        try {
            batchContainer.start();
            Communication communication = batchContainer.getJobCommunication();
            if (communication == null || communication.getState() != State.SUCCEEDED) {
                Throwable cause = communication == null ? null : communication.getThrowable();
                throw new StreamingExecutionException("bounded writer execution failed", cause);
            }
            return communication.clone();
        } finally {
            currentBatchContainer = null;
        }
    }

    private Configuration resolveBatchVariables(Configuration original, StreamingBatch batch) {
        Map<String, String> variables = new LinkedHashMap<String, String>();
        Map<String, Object> context = snapshotRunContext();
        variables.put("${stream.runId}", stringValue(context.get("stream.runId")));
        variables.put("${stream.attemptId}", stringValue(context.get("stream.attemptId")));
        variables.put("${stream.batchId}", batch.getBatchId());
        variables.put("${stream.windowStart}", Instant.ofEpochMilli(batch.getWindowStartMillis()).toString());
        Object resolved = resolveValue(original.getInternal(), variables);
        if (!(resolved instanceof Map)) {
            throw new StreamingExecutionException("streaming job configuration root must be an object");
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> resolvedMap = (Map<String, Object>) resolved;
        return Configuration.from(resolvedMap);
    }

    private Object resolveValue(Object value, Map<String, String> variables) {
        if (value instanceof Map<?, ?>) {
            Map<String, Object> result = new LinkedHashMap<String, Object>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                result.put(String.valueOf(entry.getKey()), resolveValue(entry.getValue(), variables));
            }
            return result;
        }
        if (value instanceof List<?>) {
            List<Object> result = new ArrayList<Object>();
            for (Object item : (List<?>) value) {
                result.add(resolveValue(item, variables));
            }
            return result;
        }
        if (!(value instanceof String)) {
            return value;
        }
        String resolved = (String) value;
        for (Map.Entry<String, String> variable : variables.entrySet()) {
            resolved = resolved.replace(variable.getKey(), variable.getValue());
        }
        return resolved;
    }

    private List<Record> copyRecords(List<Record> records) {
        List<Record> copies = new ArrayList<Record>(records.size());
        for (Record record : records) {
            DefaultRecord copy = new DefaultRecord();
            for (int index = 0; index < record.getColumnNumber(); index++) {
                Column column = record.getColumn(index);
                try {
                    copy.addColumn(column == null ? null : column.clone());
                } catch (CloneNotSupportedException failure) {
                    throw new StreamingExecutionException("streaming record column cannot be copied", failure);
                }
            }
            copies.add(copy);
        }
        return copies;
    }

    private StreamingPollRequest pollRequest() {
        return new StreamingPollRequest(
                configuration.getLong("streaming.pollTimeoutMs", StreamingPollRequest.DEFAULT_TIMEOUT_MILLIS),
                configuration.getInt("streaming.maxBatchRecords", StreamingPollRequest.DEFAULT_MAX_RECORDS),
                configuration.getLong("streaming.maxBatchBytes", StreamingPollRequest.DEFAULT_MAX_BYTES));
    }

    private void abort(StreamingBatch batch, Throwable failure) {
        try {
            session.abort(batch.getCheckpoint(), failure);
        } catch (RuntimeException abortFailure) {
            if (failure != null) {
                failure.addSuppressed(abortFailure);
            }
            log.warn("Streaming source abort failed for batch {}", batch.getBatchId(), abortFailure);
        }
    }

    private void sleepBeforeRetry(int failedAttempt) {
        long delay = 1_000L << Math.max(0, failedAttempt - 1);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new StreamingExecutionException("streaming batch retry was interrupted", failure);
        }
    }

    private void closeSession() {
        StreamingSession active = session;
        session = null;
        if (active != null) {
            try {
                active.close();
            } catch (RuntimeException failure) {
                log.warn("Streaming source session close failed", failure);
            }
        }
    }

    private Configuration requiredConfiguration(Configuration value, String path) {
        Configuration child = value.getConfiguration(path);
        if (child == null) {
            throw new StreamingExecutionException("streaming job requires " + path + " configuration");
        }
        return child;
    }

    private Map<String, Object> snapshotRunContext() {
        synchronized (runContext) {
            return new LinkedHashMap<String, Object>(runContext);
        }
    }

    private String stringValue(Object value) {
        return value == null ? "" : String.valueOf(value);
    }
}
