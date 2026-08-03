package com.jdragon.aggregation.core.job;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.enums.State;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.pluginloader.JarLoaderCenter;
import com.jdragon.aggregation.pluginloader.LoadUtil;
import com.jdragon.aggregation.pluginloader.constant.SystemConstants;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeResolvers;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeSession;
import com.jdragon.aggregation.pluginloader.runtime.ResolvedPlugin;
import com.jdragon.aggregation.pluginloader.type.IPluginType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JobContainerPluginRuntimeSessionTest {
    private static final IPluginType SOURCE = () -> "source";

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
    public void tearDown() {
        SystemConstants.CORE_CONFIG = originalCoreConfig;
        PluginRuntimeResolvers.reset();
        JarLoaderCenter.clearJarLoader();
    }

    @Test
    public void runningJobPinsAllPluginThreadsWhileNextJobUsesNewRevision() throws Exception {
        Path v1Directory = temporaryFolder.newFolder("source-v1").toPath();
        Path v2Directory = temporaryFolder.newFolder("source-v2").toPath();
        ResolvedPlugin v1 = new ResolvedPlugin(SOURCE, "demo", v1Directory, "revision-v1");
        ResolvedPlugin v2 = new ResolvedPlugin(SOURCE, "demo", v2Directory, "revision-v2");
        AtomicReference<ResolvedPlugin> active = new AtomicReference<ResolvedPlugin>(v1);
        PluginRuntimeResolvers.install((type, name) -> active.get());

        CountDownLatch firstJobResolved = new CountDownLatch(2);
        CountDownLatch releaseFirstJob = new CountDownLatch(1);
        List<String> firstIdentities = Collections.synchronizedList(new ArrayList<String>());
        List<String> nextIdentities = Collections.synchronizedList(new ArrayList<String>());
        JobContainer first = container(firstIdentities, firstJobResolved, releaseFirstJob);
        JobContainer next = container(nextIdentities, new CountDownLatch(2), null);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> firstRun = executor.submit(first::start);
            assertTrue("reader and writer did not resolve their plugin in time",
                    firstJobResolved.await(10, TimeUnit.SECONDS));
            active.set(v2);

            Future<?> nextRun = executor.submit(next::start);
            nextRun.get(10, TimeUnit.SECONDS);
            assertTrue(JarLoaderCenter.isDirectoryInUse(v1Directory));
            assertFalse(JarLoaderCenter.isDirectoryInUse(v2Directory));

            releaseFirstJob.countDown();
            firstRun.get(10, TimeUnit.SECONDS);
        } finally {
            releaseFirstJob.countDown();
            executor.shutdownNow();
        }

        assertEquals(List.of("revision-v1", "revision-v1"), sorted(firstIdentities));
        assertEquals(List.of("revision-v2", "revision-v2"), sorted(nextIdentities));
        assertEquals("revision-v1", pluginRevision(first));
        assertEquals("revision-v2", pluginRevision(next));
        assertNull(first.getPluginRuntimeSession());
        assertNull(next.getPluginRuntimeSession());
        assertFalse(JarLoaderCenter.isDirectoryInUse(v1Directory));
        assertFalse(JarLoaderCenter.isDirectoryInUse(v2Directory));
    }

    @Test
    public void failedJobStillReleasesItsRuntimeSession() throws Exception {
        Path directory = temporaryFolder.newFolder("failed-source").toPath();
        ResolvedPlugin revision = new ResolvedPlugin(SOURCE, "demo", directory, "failed-revision");
        PluginRuntimeResolvers.install((type, name) -> revision);
        CountDownLatch resolved = new CountDownLatch(2);
        List<String> identities = Collections.synchronizedList(new ArrayList<String>());
        JobContainer container = container(identities, resolved, null);
        container.addConsumerPlugin(PluginType.READER,
                (pluginConfiguration, peerConfiguration) -> new Reader.Job() {
                    @Override
                    public void startRead(RecordSender recordSender) {
                        identities.add(resolveIdentity());
                        resolved.countDown();
                        throw new IllegalStateException("expected reader failure");
                    }
                });

        container.start();

        assertTrue(resolved.await(5, TimeUnit.SECONDS));
        assertEquals("failed-revision", pluginRevision(container));
        assertNull(container.getPluginRuntimeSession());
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (JarLoaderCenter.isDirectoryInUse(directory) && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertFalse(JarLoaderCenter.isDirectoryInUse(directory));
    }

    @Test
    public void initializationFailureStillReleasesItsRuntimeSession() {
        JobContainer container = container(new ArrayList<String>(), new CountDownLatch(2), null);
        container.setRunContext("jobId", "not-a-long");

        try {
            container.start();
            fail("expected invalid jobId to fail initialization");
        } catch (ClassCastException expected) {
            // The invalid runtime context fails before plugin threads start.
        }

        assertNull(PluginRuntimeSession.current());
        assertNull(container.getPluginRuntimeSession());
    }

    @Test
    public void runtimeInitializationFailureStopsReporterThread() throws Exception {
        Configuration configuration = Configuration.from("{"
                + "\"core\":{\"plugin\":{\"reporter\":[]}}}");
        JobContainer container = new JobContainer(configuration);

        try {
            container.start();
            fail("expected missing reader configuration to fail initialization");
        } catch (NullPointerException expected) {
            // The reporter has already started when startJob reads the missing reader.
        }

        Thread reporter = container.getReporterThread();
        assertTrue("reporter thread was not started", reporter != null);
        reporter.join(TimeUnit.SECONDS.toMillis(2));
        boolean reporterStopped = !reporter.isAlive();
        if (!reporterStopped) {
            reporter.interrupt();
            reporter.join(TimeUnit.SECONDS.toMillis(2));
        }

        assertTrue("reporter thread survived the initialization failure", reporterStopped);
        assertEquals(State.FAILED, container.getJobPointReporter().getTrackCommunication().getState());
        assertNull(PluginRuntimeSession.current());
        assertNull(container.getPluginRuntimeSession());
    }

    private JobContainer container(List<String> identities,
                                   CountDownLatch resolved,
                                   CountDownLatch readerGate) {
        Configuration configuration = Configuration.from("{"
                + "\"core\":{\"plugin\":{\"reporter\":[]},"
                + "\"container\":{\"taskGroup\":{\"reportInterval\":20,\"sleepInterval\":5}}},"
                + "\"reader\":{\"type\":\"controlled\",\"config\":{}},"
                + "\"writer\":{\"type\":\"controlled\",\"config\":{}}}");
        JobContainer container = new JobContainer(configuration);
        container.addConsumerPlugin(PluginType.READER,
                (pluginConfiguration, peerConfiguration) ->
                        new ControlledReader(identities, resolved, readerGate));
        container.addConsumerPlugin(PluginType.WRITER,
                (pluginConfiguration, peerConfiguration) ->
                        new ControlledWriter(identities, resolved));
        return container;
    }

    @SuppressWarnings("unchecked")
    private String pluginRevision(JobContainer container) {
        Map<String, String> revisions = (Map<String, String>) container.getRunContext().get("pluginRevisions");
        return revisions.get("source/demo");
    }

    private List<String> sorted(List<String> identities) {
        List<String> result = new ArrayList<String>(identities);
        Collections.sort(result);
        return result;
    }

    private static String resolveIdentity() {
        return LoadUtil.resolvePlugin(SOURCE, "demo").getIdentity();
    }

    private static final class ControlledReader extends Reader.Job {
        private final List<String> identities;
        private final CountDownLatch resolved;
        private final CountDownLatch gate;

        private ControlledReader(List<String> identities, CountDownLatch resolved, CountDownLatch gate) {
            this.identities = identities;
            this.resolved = resolved;
            this.gate = gate;
        }

        @Override
        public void startRead(RecordSender recordSender) {
            identities.add(resolveIdentity());
            resolved.countDown();
            if (gate != null) {
                try {
                    assertTrue("test gate timed out", gate.await(10, TimeUnit.SECONDS));
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(ex);
                }
            }
        }
    }

    private static final class ControlledWriter extends Writer.Job {
        private final List<String> identities;
        private final CountDownLatch resolved;

        private ControlledWriter(List<String> identities, CountDownLatch resolved) {
            this.identities = identities;
            this.resolved = resolved;
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            identities.add(resolveIdentity());
            resolved.countDown();
            while (lineReceiver.getFromReader() != null) {
                // Drain until the reader terminates the channel.
            }
        }
    }
}
