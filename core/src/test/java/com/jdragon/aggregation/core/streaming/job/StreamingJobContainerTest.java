package com.jdragon.aggregation.core.streaming.job;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.enums.State;
import com.jdragon.aggregation.core.job.JobContainer;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.pluginloader.constant.SystemConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StreamingJobContainerTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private String originalCoreConfig;

    @Before
    public void setUpCoreConfig() throws Exception {
        originalCoreConfig = SystemConstants.CORE_CONFIG;
        Path coreConfig = temporaryFolder.newFile("core.json").toPath();
        Files.writeString(coreConfig, "{}", StandardCharsets.UTF_8);
        SystemConstants.CORE_CONFIG = coreConfig.toString();
    }

    @After
    public void restoreCoreConfig() {
        SystemConstants.CORE_CONFIG = originalCoreConfig;
    }

    @Test
    public void oldWriterCompletesBeforeCheckpointIsCommitted() throws Exception {
        TestSource source = new TestSource(batch("batch-1", "alpha", "beta"));
        List<String> written = Collections.synchronizedList(new ArrayList<String>());
        AtomicReference<String> outputPath = new AtomicReference<String>();
        StreamingJobContainer container = new StreamingJobContainer(configuration(), source);
        container.setRunContext("stream.runId", 11L);
        container.setRunContext("stream.attemptId", 22L);
        container.addConsumerPlugin(PluginType.WRITER,
                (pluginConfiguration, peerConfiguration) -> new Writer.Job() {
                    @Override
                    public void init() {
                        outputPath.set(getPluginJobConf().getString("outputPath"));
                    }

                    @Override
                    public void startWrite(RecordReceiver receiver) {
                        Record record;
                        while ((record = receiver.getFromReader()) != null) {
                            written.add(record.getColumn(0).asString());
                        }
                    }
                });

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> future = executor.submit(container::start);
            assertTrue(source.committed.await(5, TimeUnit.SECONDS));
            container.requestStop();
            future.get(5, TimeUnit.SECONDS);
        } finally {
            container.cancel();
            executor.shutdownNow();
        }

        assertEquals(List.of("alpha", "beta"), written);
        assertEquals("runs/11/22/batch-1.json", outputPath.get());
        assertEquals(1, source.commitCount.get());
        assertEquals(0, source.abortCount.get());
        assertEquals("batch-1", container.getLastCommittedCheckpoint().getBatchId());
    }

    @Test
    public void failedOldWriterIsRetriedWithoutCommittingCheckpoint() {
        TestSource source = new TestSource(batch("batch-fail", "alpha"));
        AtomicInteger writerRuns = new AtomicInteger();
        StreamingJobContainer container = new StreamingJobContainer(configuration(), source);
        container.addConsumerPlugin(PluginType.WRITER,
                (pluginConfiguration, peerConfiguration) -> new Writer.Job() {
                    @Override
                    public void startWrite(RecordReceiver receiver) {
                        writerRuns.incrementAndGet();
                        throw new IllegalStateException("expected writer failure");
                    }
                });

        try {
            container.start();
            fail("expected streaming batch failure");
        } catch (StreamingExecutionException expected) {
            assertTrue(expected.getMessage().contains("batch-fail"));
        }

        assertEquals(4, writerRuns.get());
        assertEquals(0, source.commitCount.get());
        assertEquals(1, source.abortCount.get());
    }

    @Test
    public void checkedCommitFailureAbortsWithoutAdvancingCheckpoint() {
        TestSource source = new TestSource(batch("batch-commit-fail", "alpha"));
        source.commitFailure = new Exception("expected commit failure");
        Configuration configuration = configuration();
        configuration.set("streaming.batchRetryCount", 0);
        StreamingJobContainer container = new StreamingJobContainer(configuration, source);
        container.addConsumerPlugin(PluginType.WRITER,
                (pluginConfiguration, peerConfiguration) -> new Writer.Job() {
                    @Override
                    public void startWrite(RecordReceiver receiver) {
                        while (receiver.getFromReader() != null) {
                        }
                    }
                });

        try {
            container.start();
            fail("expected streaming commit failure");
        } catch (StreamingExecutionException expected) {
            assertTrue(expected.getMessage().contains("batch-commit-fail"));
        }

        assertEquals(1, source.commitCount.get());
        assertEquals(1, source.abortCount.get());
        assertTrue(container.getLastCommittedCheckpoint() == null);
    }

    @Test
    public void postCommitListenerFailureDoesNotRetryWriterOrAbortCommittedBatch() {
        TestSource source = new TestSource(batch("batch-listener-fail", "alpha"));
        AtomicInteger writerRuns = new AtomicInteger();
        AtomicInteger listenerRuns = new AtomicInteger();
        StreamingJobContainer container = new StreamingJobContainer(configuration(), source);
        container.addConsumerPlugin(PluginType.WRITER,
                (pluginConfiguration, peerConfiguration) -> new Writer.Job() {
                    @Override
                    public void startWrite(RecordReceiver receiver) {
                        writerRuns.incrementAndGet();
                        while (receiver.getFromReader() != null) {
                        }
                    }
                });
        container.setListener(new StreamingJobListener() {
            @Override
            public void onBatchCommitted(StreamingBatch batch,
                                         com.jdragon.aggregation.core.statistics.communication.Communication communication,
                                         long durationMillis) {
                listenerRuns.incrementAndGet();
                throw new IllegalStateException("expected checkpoint listener failure");
            }
        });

        try {
            container.start();
            fail("expected post-commit listener failure");
        } catch (StreamingExecutionException expected) {
            assertTrue(expected.getMessage().contains("post-commit listener failed"));
        }

        assertEquals(1, writerRuns.get());
        assertEquals(1, listenerRuns.get());
        assertEquals(1, source.commitCount.get());
        assertEquals(0, source.abortCount.get());
        assertEquals("batch-listener-fail", container.getLastCommittedCheckpoint().getBatchId());
    }

    @Test
    public void cancellationAbortsInFlightBatchWithoutCommitting() throws Exception {
        TestSource source = new TestSource(batch("batch-cancel", "alpha"));
        CountDownLatch writerStarted = new CountDownLatch(1);
        StreamingJobContainer container = new StreamingJobContainer(configuration(), source);
        container.addConsumerPlugin(PluginType.WRITER,
                (pluginConfiguration, peerConfiguration) -> new Writer.Job() {
                    @Override
                    public void startWrite(RecordReceiver receiver) {
                        receiver.getFromReader();
                        writerStarted.countDown();
                        while (!Thread.currentThread().isInterrupted()) {
                            try {
                                Thread.sleep(20L);
                            } catch (InterruptedException failure) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                });

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> future = executor.submit(container::start);
            assertTrue(writerStarted.await(5, TimeUnit.SECONDS));
            container.cancel();
            future.get(5, TimeUnit.SECONDS);
        } finally {
            container.cancel();
            executor.shutdownNow();
        }

        assertTrue(container.isCancellationRequested());
        assertEquals(0, source.commitCount.get());
        assertEquals(1, source.abortCount.get());
        assertTrue(container.getLastCommittedCheckpoint() == null);
    }

    @Test
    public void jobContainerCancelIsIdempotentAndDestroysPluginsOnce() throws Exception {
        CountDownLatch readerStarted = new CountDownLatch(1);
        AtomicInteger readerDestroyed = new AtomicInteger();
        AtomicInteger writerDestroyed = new AtomicInteger();
        JobContainer container = new JobContainer(configuration());
        container.addConsumerPlugin(PluginType.READER,
                (pluginConfiguration, peerConfiguration) -> new Reader.Job() {
                    @Override
                    public void startRead(RecordSender sender) {
                        readerStarted.countDown();
                        while (!Thread.currentThread().isInterrupted()) {
                            try {
                                Thread.sleep(20L);
                            } catch (InterruptedException failure) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }

                    @Override
                    public void destroy() {
                        readerDestroyed.incrementAndGet();
                    }
                });
        container.addConsumerPlugin(PluginType.WRITER,
                (pluginConfiguration, peerConfiguration) -> new Writer.Job() {
                    @Override
                    public void startWrite(RecordReceiver receiver) {
                        while (receiver.getFromReader() != null) {
                        }
                    }

                    @Override
                    public void destroy() {
                        writerDestroyed.incrementAndGet();
                    }
                });

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> future = executor.submit(container::start);
            assertTrue(readerStarted.await(5, TimeUnit.SECONDS));
            container.cancel();
            container.cancel();
            future.get(5, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        assertEquals(State.KILLED, container.getJobPointReporter().getTrackCommunication().getState());
        assertEquals(State.KILLED, container.getJobCommunication().clone().getState());
        assertEquals(1, readerDestroyed.get());
        assertEquals(1, writerDestroyed.get());
    }

    private Configuration configuration() {
        return Configuration.from("{"
                + "\"core\":{\"plugin\":{\"reporter\":[]},"
                + "\"container\":{\"taskGroup\":{\"reportInterval\":20,\"sleepInterval\":5}}},"
                + "\"streaming\":{\"pollTimeoutMs\":20,\"batchRetryCount\":3},"
                + "\"reader\":{\"type\":\"test\",\"config\":{}},"
                + "\"writer\":{\"type\":\"test\",\"config\":{"
                + "\"outputPath\":\"runs/${stream.runId}/${stream.attemptId}/${stream.batchId}.json\"}}}");
    }

    private StreamingBatch batch(String batchId, String... values) {
        List<Record> records = new ArrayList<Record>();
        for (String value : values) {
            DefaultRecord record = new DefaultRecord();
            record.addColumn(new StringColumn(value));
            records.add(record);
        }
        return new StreamingBatch(records,
                new StreamingCheckpoint(batchId, Map.<String, Object>of("offset", values.length)),
                1_700_000_000_000L, 0L);
    }

    private static final class TestSource implements StreamingSource {
        private final StreamingBatch batch;
        private final CountDownLatch committed = new CountDownLatch(1);
        private final AtomicInteger commitCount = new AtomicInteger();
        private final AtomicInteger abortCount = new AtomicInteger();
        private volatile boolean delivered;
        private volatile boolean wakeup;
        private volatile Exception commitFailure;

        private TestSource(StreamingBatch batch) {
            this.batch = batch;
        }

        @Override
        public StreamingSession open(StreamingSourceContext context) {
            return new StreamingSession() {
                @Override
                public StreamingBatch pollBatch(StreamingPollRequest request) {
                    if (!delivered) {
                        delivered = true;
                        return batch;
                    }
                    if (!wakeup) {
                        try {
                            Thread.sleep(Math.min(20L, request.getTimeoutMillis()));
                        } catch (InterruptedException failure) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    return StreamingBatch.empty();
                }

                @Override
                public void commit(StreamingCheckpoint checkpoint) throws Exception {
                    assertSame(batch.getCheckpoint(), checkpoint);
                    commitCount.incrementAndGet();
                    if (commitFailure != null) {
                        throw commitFailure;
                    }
                    committed.countDown();
                }

                @Override
                public void abort(StreamingCheckpoint checkpoint, Throwable failure) {
                    abortCount.incrementAndGet();
                }

                @Override
                public void wakeup() {
                    wakeup = true;
                }

                @Override
                public void close() {
                }
            };
        }
    }
}
