package com.jdragon.aggregation.pluginloader.runtime;

import com.jdragon.aggregation.pluginloader.JarLoaderCenter;
import com.jdragon.aggregation.pluginloader.LoadUtil;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import com.jdragon.aggregation.pluginloader.ConfigParser;
import com.jdragon.aggregation.pluginloader.constant.SystemConstants;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import com.jdragon.aggregation.pluginloader.type.IPluginType;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PluginRuntimeSessionTest {
    private static final IPluginType READER = () -> "reader";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final String originalPluginHome = SystemConstants.PLUGIN_HOME;

    @After
    public void tearDown() {
        PluginRuntimeResolvers.reset();
        JarLoaderCenter.clearJarLoader();
        SystemConstants.PLUGIN_HOME = originalPluginHome;
    }

    @Test
    public void localResolverKeepsStandaloneDirectoryLayout() throws Exception {
        Path pluginHome = temporaryFolder.newFolder("aggregation", "plugin").toPath();
        SystemConstants.PLUGIN_HOME = pluginHome.toString();

        ResolvedPlugin resolved = LoadUtil.resolvePlugin(READER, "demo");

        assertEquals(pluginHome.resolve("reader").resolve("demo").toAbsolutePath().normalize(),
                resolved.getDirectory());
        assertTrue(resolved.getIdentity().startsWith("local:"));
    }

    @Test
    public void missingLocalPluginKeepsLegacyLoadingFailureAndDoesNotLeakSession() throws Exception {
        Path pluginHome = temporaryFolder.newFolder("missing-aggregation", "plugin").toPath();
        SystemConstants.PLUGIN_HOME = pluginHome.toString();

        try {
            PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(READER, "missing");
            fail("Expected the missing local plugin directory to fail");
        } catch (RuntimeException expected) {
            assertTrue(expected.getMessage().contains("jar"));
        }

        assertNull(PluginRuntimeSession.current());
    }

    @Test
    public void oneSessionPinsIdentityWhileNextSessionSeesNewRevision() throws Exception {
        Path v1Directory = temporaryFolder.newFolder("v1").toPath();
        Path v2Directory = temporaryFolder.newFolder("v2").toPath();
        ResolvedPlugin v1 = new ResolvedPlugin(READER, "demo", v1Directory, "revision-v1");
        ResolvedPlugin v2 = new ResolvedPlugin(READER, "demo", v2Directory, "revision-v2");
        AtomicReference<ResolvedPlugin> active = new AtomicReference<ResolvedPlugin>(v1);
        PluginRuntimeResolvers.install((type, name) -> active.get());

        PluginRuntimeSession first = PluginRuntimeSession.createDetached();
        PluginRuntimeSession second = PluginRuntimeSession.createDetached();
        try {
            ResolvedPlugin firstResolution = first.call(() -> LoadUtil.resolvePlugin(READER, "demo"));
            active.set(v2);
            ResolvedPlugin pinnedResolution = first.call(() -> LoadUtil.resolvePlugin(READER, "demo"));
            ResolvedPlugin nextResolution = second.call(() -> LoadUtil.resolvePlugin(READER, "demo"));

            assertSame(firstResolution, pinnedResolution);
            assertEquals("revision-v1", pinnedResolution.getIdentity());
            assertEquals("revision-v2", nextResolution.getIdentity());
        } finally {
            first.close();
            second.close();
        }
    }

    @Test
    public void retiredLoaderClosesOnlyAfterLastLeaseReleases() throws Exception {
        Path v1Directory = temporaryFolder.newFolder("loader-v1").toPath();
        Path v2Directory = temporaryFolder.newFolder("loader-v2").toPath();
        ResolvedPlugin v1 = new ResolvedPlugin(READER, "demo", v1Directory, "revision-v1");
        ResolvedPlugin v2 = new ResolvedPlugin(READER, "demo", v2Directory, "revision-v2");

        JarLoaderCenter.LoaderLease firstLease = JarLoaderCenter.acquire(v1);
        JarLoaderCenter.LoaderLease secondLease = JarLoaderCenter.acquire(v2);
        assertTrue(JarLoaderCenter.isDirectoryInUse(v1Directory));
        assertTrue(JarLoaderCenter.isDirectoryInUse(v2Directory));

        firstLease.close();
        assertFalse(JarLoaderCenter.isDirectoryInUse(v1Directory));
        assertTrue(JarLoaderCenter.isDirectoryInUse(v2Directory));

        secondLease.close();
        assertFalse(JarLoaderCenter.isDirectoryInUse(v2Directory));
    }

    @Test
    public void realPluginClassesAndConfigsStayIsolatedByIdentity() throws Exception {
        Path v1Directory = createVersionedPlugin("real-v1", "v1");
        Path v2Directory = createVersionedPlugin("real-v2", "v2");
        ResolvedPlugin v1 = new ResolvedPlugin(READER, "demo", v1Directory, "revision-v1");
        ResolvedPlugin v2 = new ResolvedPlugin(READER, "demo", v2Directory, "revision-v2");
        AtomicReference<ResolvedPlugin> active = new AtomicReference<ResolvedPlugin>(v1);
        PluginRuntimeResolvers.install((type, name) -> active.get());

        PluginRuntimeSession firstTask = PluginRuntimeSession.createDetached();
        PluginRuntimeSession nextTask = PluginRuntimeSession.createDetached();
        try {
            AbstractPlugin first = firstTask.call(() -> LoadUtil.loadJobPlugin(READER, "demo"));
            String firstMarker = firstTask.call(() -> ConfigParser
                    .parseOnePluginConfig(READER, "demo")
                    .getString("plugin.reader.demo.marker"));

            active.set(v2);
            AbstractPlugin stillFirst = firstTask.call(() -> LoadUtil.loadJobPlugin(READER, "demo"));
            AbstractPlugin second = nextTask.call(() -> LoadUtil.loadJobPlugin(READER, "demo"));
            String secondMarker = nextTask.call(() -> ConfigParser
                    .parseOnePluginConfig(READER, "demo")
                    .getString("plugin.reader.demo.marker"));

            assertEquals("v1", invokeVersion(first));
            assertEquals("v1", invokeVersion(stillFirst));
            assertEquals("v2", invokeVersion(second));
            assertEquals("v1", firstMarker);
            assertEquals("v2", secondMarker);
            assertSame(first.getClass().getClassLoader(), stillFirst.getClass().getClassLoader());
            assertFalse(first.getClass().getClassLoader() == second.getClass().getClassLoader());
            assertTrue(JarLoaderCenter.isDirectoryInUse(v1Directory));
            assertTrue(JarLoaderCenter.isDirectoryInUse(v2Directory));

            firstTask.close();
            assertFalse(JarLoaderCenter.isDirectoryInUse(v1Directory));
            assertTrue(JarLoaderCenter.isDirectoryInUse(v2Directory));
        } finally {
            if (!firstTask.isClosed()) {
                firstTask.close();
            }
            nextTask.close();
        }
        assertFalse(JarLoaderCenter.isDirectoryInUse(v2Directory));
    }

    @Test
    public void failedOperationResolutionDoesNotLeakThreadSession() {
        PluginRuntimeResolvers.install((type, name) -> {
            throw new IllegalStateException("resolver unavailable");
        });

        try {
            PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(READER, "demo");
            fail("Expected plugin resolution to fail");
        } catch (IllegalStateException expected) {
            assertEquals("resolver unavailable", expected.getMessage());
        }

        assertNull(PluginRuntimeSession.current());
    }

    @Test
    public void closedSessionDoesNotLeakIntoReusedExecutorWorker() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        PluginRuntimeSession session = PluginRuntimeSession.open();
        try {
            executor.submit(session.bind(() -> assertSame(session, PluginRuntimeSession.current())))
                    .get(5, TimeUnit.SECONDS);

            session.close();
            assertNull(executor.submit(PluginRuntimeSession::current).get(5, TimeUnit.SECONDS));
        } finally {
            session.close();
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    public void closeCannotReleaseLeasesWhileCloseAfterThreadsIsWaiting() throws Exception {
        Path pluginDirectory = temporaryFolder.newFolder("deferred-close").toPath();
        ResolvedPlugin plugin = new ResolvedPlugin(READER, "demo", pluginDirectory, "revision-v1");
        PluginRuntimeResolvers.install((type, name) -> plugin);

        PluginRuntimeSession session = PluginRuntimeSession.createDetached();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        Thread pluginThread = new Thread(session.bind(() -> {
            started.countDown();
            try {
                finish.await();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }), "plugin-runtime-deferred-close-test");

        try {
            session.call(() -> LoadUtil.resolvePlugin(READER, "demo"));
            pluginThread.start();
            assertTrue(started.await(5, TimeUnit.SECONDS));

            session.closeAfterThreads(pluginThread);
            session.close();

            assertFalse(session.isClosed());
            assertTrue(JarLoaderCenter.isDirectoryInUse(pluginDirectory));
            try {
                session.call(() -> null);
                fail("A closing session must reject new work");
            } catch (IllegalStateException expected) {
                assertTrue(expected.getMessage().contains("closing"));
            }

            finish.countDown();
            pluginThread.join(TimeUnit.SECONDS.toMillis(5));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!session.isClosed() && System.nanoTime() < deadline) {
                Thread.sleep(10L);
            }

            assertTrue(session.isClosed());
            assertFalse(JarLoaderCenter.isDirectoryInUse(pluginDirectory));
        } finally {
            finish.countDown();
            pluginThread.join(TimeUnit.SECONDS.toMillis(5));
            session.close();
        }
    }

    private Path createVersionedPlugin(String directoryName, String version) throws Exception {
        Path pluginDirectory = temporaryFolder.newFolder(directoryName).toPath();
        Path sourceRoot = temporaryFolder.newFolder(directoryName + "-source").toPath();
        Path classesRoot = temporaryFolder.newFolder(directoryName + "-classes").toPath();
        Path source = sourceRoot.resolve("example").resolve("VersionedPlugin.java");
        Files.createDirectories(source.getParent());
        Files.writeString(source,
                "package example;\n"
                        + "public final class VersionedPlugin extends "
                        + "com.jdragon.aggregation.pluginloader.spi.AbstractPlugin {\n"
                        + "  public String version() { return \"" + version + "\"; }\n"
                        + "}\n",
                StandardCharsets.UTF_8);
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertTrue("Tests require a JDK, not a JRE", compiler != null);
        int compileResult = compiler.run(null, null, null,
                "-classpath", System.getProperty("java.class.path"),
                "-d", classesRoot.toString(), source.toString());
        assertEquals("Synthetic plugin compilation failed", 0, compileResult);

        Path jar = pluginDirectory.resolve("demo.jar");
        try (JarOutputStream output = new JarOutputStream(Files.newOutputStream(jar))) {
            Path classFile = classesRoot.resolve("example").resolve("VersionedPlugin.class");
            output.putNextEntry(new JarEntry("example/VersionedPlugin.class"));
            Files.copy(classFile, output);
            output.closeEntry();
        }
        Files.writeString(pluginDirectory.resolve("plugin.json"),
                "{\"name\":\"demo\",\"class\":\"example.VersionedPlugin\","
                        + "\"marker\":\"" + version + "\"}",
                StandardCharsets.UTF_8);
        return pluginDirectory;
    }

    private String invokeVersion(AbstractPlugin plugin) {
        try {
            return String.valueOf(plugin.getClass().getMethod("version").invoke(plugin));
        } catch (Exception ex) {
            throw new AssertionError("Failed to call synthetic plugin", ex);
        }
    }
}
