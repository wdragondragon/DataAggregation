package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.local.LocalSourcePlugin;
import com.jdragon.aggregation.datasource.file.transfer.StorageCapabilities;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.datasource.file.transfer.TransferFilePage;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.datasource.file.transfer.TransferWriteSession;
import com.jdragon.aggregation.transfer.model.ConflictPolicy;
import com.jdragon.aggregation.transfer.model.SourceSuccessAction;
import com.jdragon.aggregation.transfer.model.TransferCheckpoint;
import com.jdragon.aggregation.transfer.model.TransferEndpoint;
import com.jdragon.aggregation.transfer.model.TransferItemStatus;
import com.jdragon.aggregation.transfer.model.TransferMapping;
import com.jdragon.aggregation.transfer.model.TransferPlanItem;
import com.jdragon.aggregation.transfer.model.TransferPolicy;
import com.jdragon.aggregation.transfer.model.TransferResult;
import com.jdragon.aggregation.transfer.model.TransferRunStatus;
import com.jdragon.aggregation.transfer.model.TransferRuntimeOptions;
import com.jdragon.aggregation.transfer.model.TransferSelection;
import com.jdragon.aggregation.transfer.model.TransferSpec;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TransferEngineLocalIntegrationTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void discoversFiltersAndTransfersMultipleFiles() throws Exception {
        Path source = temporaryFolder.newFolder("multi-source").toPath();
        Path target = temporaryFolder.newFolder("multi-target").toPath();
        Files.createDirectories(source.resolve("docs/nested"));
        Files.writeString(source.resolve("root.txt"), "root");
        Files.writeString(source.resolve("docs/a.txt"), "alpha");
        Files.writeString(source.resolve("docs/nested/b.txt"), "beta");
        Files.writeString(source.resolve("docs/ignored.log"), "ignored");

        TransferSpec spec = spec(source, target, List.of(), List.of("**/*.txt"),
                TransferPolicy.defaults(), runtime(2));
        PreparedTransfer prepared = prepare(spec, "run-multi");
        TransferResult result = execute(prepared, source, target, new InMemoryTransferCheckpointStore());

        assertEquals(TransferRunStatus.SUCCESS, result.status());
        assertEquals(3, result.totalFiles());
        assertEquals("root", Files.readString(target.resolve("root.txt")));
        assertEquals("alpha", Files.readString(target.resolve("docs/a.txt")));
        assertEquals("beta", Files.readString(target.resolve("docs/nested/b.txt")));
        assertFalse(Files.exists(target.resolve("docs/ignored.log")));
    }

    @Test
    public void doesNotDuplicateLogicalObjectKeyWhenRootIsSelected() throws Exception {
        String key = "aggregation-plugins/production/source/dm/releases/20260730-v1/plugin.zip";
        ObjectKeyFileSystem source = new ObjectKeyFileSystem(key);
        TransferSpec spec = new TransferSpec(1,
                new TransferEndpoint("aliyun-oss", "oss-source", Map.of("rootPath", key)),
                new TransferEndpoint("local", "target", Map.of()),
                new TransferSelection(key, List.of(), false, List.of(), null, List.of(),
                        null, null, null, null, 100),
                new TransferMapping("/target", true, null), TransferPolicy.defaults(), runtime(1),
                "UTC", Map.of());

        PreparedTransfer prepared = new TransferPlanner().prepare(spec, source, "run-oss-root",
                Instant.parse("2026-08-07T00:00:00Z"), TransferEventListener.NOOP);

        assertEquals(1, prepared.plan().items().size());
        assertEquals(key, prepared.plan().items().get(0).sourcePath());
        assertEquals(List.of(key, key), source.statCalls);
    }

    @Test
    public void alwaysSkipsExistingTargetWithSameSha256() throws Exception {
        Path source = temporaryFolder.newFolder("skip-source").toPath();
        Path target = temporaryFolder.newFolder("skip-target").toPath();
        Files.writeString(source.resolve("same.bin"), "same-content");
        Files.writeString(target.resolve("same.bin"), "same-content");

        PreparedTransfer prepared = prepare(spec(source, target, List.of("same.bin"), List.of(),
                new TransferPolicy(ConflictPolicy.OVERWRITE, "SHA-256",
                        SourceSuccessAction.DELETE, null), runtime(1)), "run-skip");
        TransferResult result = execute(prepared, source, target, new InMemoryTransferCheckpointStore());

        assertEquals(TransferRunStatus.SUCCESS, result.status());
        assertEquals(TransferItemStatus.SKIPPED, result.items().get(0).status());
        assertTrue(Files.exists(source.resolve("same.bin")));
        assertEquals(0, result.transferredBytes());
    }

    @Test
    public void opensTargetOnlyAfterSourceChecksumCompletes() throws Exception {
        Path source = temporaryFolder.newFolder("source-before-target-source").toPath();
        Path target = temporaryFolder.newFolder("source-before-target-target").toPath();
        Files.writeString(source.resolve("item.txt"), "payload");
        PreparedTransfer prepared = prepare(spec(source, target, List.of("item.txt"), List.of(),
                TransferPolicy.defaults(), runtime(1)), "run-source-before-target");
        AtomicBoolean targetOpened = new AtomicBoolean();

        TransferResult result = new TransferEngine().execute(prepared, () -> {
            Configuration configuration = Configuration.newDefault();
            configuration.set("rootPath", source.toString());
            LocalSourcePlugin plugin = new LocalSourcePlugin() {
                @Override
                public String checksum(String path, String algorithm) throws java.io.IOException {
                    assertFalse("target must stay closed while source checksum is running",
                            targetOpened.get());
                    return super.checksum(path, algorithm);
                }
            };
            plugin.connect(configuration);
            return plugin;
        }, () -> {
            targetOpened.set(true);
            return connected(target);
        }, new InMemoryTransferCheckpointStore(), TransferEventListener.NOOP, new TransferControl());

        assertEquals(TransferRunStatus.SUCCESS, result.status());
        assertTrue(targetOpened.get());
    }

    @Test
    public void conflictPolicyFailPreservesBothFiles() throws Exception {
        Path source = temporaryFolder.newFolder("conflict-source").toPath();
        Path target = temporaryFolder.newFolder("conflict-target").toPath();
        Files.writeString(source.resolve("item.txt"), "source");
        Files.writeString(target.resolve("item.txt"), "target");

        PreparedTransfer prepared = prepare(spec(source, target, List.of("item.txt"), List.of(),
                TransferPolicy.defaults(), runtime(1)), "run-conflict");
        TransferResult result = execute(prepared, source, target, new InMemoryTransferCheckpointStore());

        assertEquals(TransferRunStatus.FAILED, result.status());
        assertEquals(TransferItemStatus.CONFLICT, result.items().get(0).status());
        assertEquals("source", Files.readString(source.resolve("item.txt")));
        assertEquals("target", Files.readString(target.resolve("item.txt")));
    }

    @Test
    public void backsUpDifferentTargetBeforeOverwrite() throws Exception {
        Path source = temporaryFolder.newFolder("backup-source").toPath();
        Path target = temporaryFolder.newFolder("backup-target").toPath();
        Files.writeString(source.resolve("item.txt"), "new-content");
        Files.writeString(target.resolve("item.txt"), "old-content");
        TransferPolicy policy = new TransferPolicy(ConflictPolicy.BACKUP_THEN_OVERWRITE,
                "SHA-256", SourceSuccessAction.KEEP, null);

        TransferResult result = execute(prepare(spec(source, target, List.of("item.txt"), List.of(),
                policy, runtime(1)), "run-backup"), source, target, new InMemoryTransferCheckpointStore());

        assertEquals(TransferRunStatus.SUCCESS, result.status());
        assertEquals("new-content", Files.readString(target.resolve("item.txt")));
        try (java.util.stream.Stream<Path> files = Files.list(target)) {
            Path backup = files.filter(path -> path.getFileName().toString().startsWith("item.txt.backup."))
                    .findFirst().orElseThrow();
            assertEquals("old-content", Files.readString(backup));
        }
    }

    @Test
    public void resumesOnlyWhenCheckpointAndTemporaryOffsetMatch() throws Exception {
        Path source = temporaryFolder.newFolder("resume-source").toPath();
        Path target = temporaryFolder.newFolder("resume-target").toPath();
        Files.writeString(source.resolve("item.txt"), "abcdefghij");
        PreparedTransfer prepared = prepare(spec(source, target, List.of("item.txt"), List.of(),
                TransferPolicy.defaults(), runtime(1)), "run-resume");
        TransferPlanItem item = prepared.plan().items().get(0);
        InMemoryTransferCheckpointStore checkpoints = new InMemoryTransferCheckpointStore();

        try (LocalSourcePlugin targetPlugin = connected(target)) {
            TransferWriteSession session = targetPlugin.prepareWrite(item.temporaryPath(), item.itemId());
            assertEquals(4, targetPlugin.append(session, 0,
                    new ByteArrayInputStream("abcd".getBytes(StandardCharsets.UTF_8)), 4));
        }
        checkpoints.save(new TransferCheckpoint(prepared.plan().runId(), item.itemId(),
                item.sourceIdentity(), item.targetIdentity(), item.sourceSnapshot(), item.temporaryPath(),
                4, Instant.now()));

        TransferResult result = execute(prepared, source, target, checkpoints);

        assertEquals(TransferRunStatus.SUCCESS, result.status());
        assertEquals(4, result.items().get(0).resumedBytes());
        assertEquals("abcdefghij", Files.readString(target.resolve("item.txt")));
        assertTrue(checkpoints.load(prepared.plan().runId(), item.itemId()).isEmpty());
    }

    @Test
    public void rejectsExternallyChangedTemporaryTarget() throws Exception {
        Path source = temporaryFolder.newFolder("invalid-source").toPath();
        Path target = temporaryFolder.newFolder("invalid-target").toPath();
        Files.writeString(source.resolve("item.txt"), "abcdefghij");
        PreparedTransfer prepared = prepare(spec(source, target, List.of("item.txt"), List.of(),
                TransferPolicy.defaults(), runtime(1)), "run-invalid-checkpoint");
        TransferPlanItem item = prepared.plan().items().get(0);
        InMemoryTransferCheckpointStore checkpoints = new InMemoryTransferCheckpointStore();
        Files.writeString(Path.of(item.temporaryPath()), "abcdef");
        checkpoints.save(new TransferCheckpoint(prepared.plan().runId(), item.itemId(),
                item.sourceIdentity(), item.targetIdentity(), item.sourceSnapshot(), item.temporaryPath(),
                4, Instant.now()));

        TransferResult result = execute(prepared, source, target, checkpoints);

        assertEquals(TransferRunStatus.FAILED, result.status());
        assertEquals("CHECKPOINT_INVALID", result.items().get(0).errorCode());
        assertFalse(Files.exists(target.resolve("item.txt")));
    }

    @Test
    public void deletesSourceOnlyAfterSuccessfulCommit() throws Exception {
        Path source = temporaryFolder.newFolder("delete-source").toPath();
        Path target = temporaryFolder.newFolder("delete-target").toPath();
        Files.writeString(source.resolve("item.txt"), "payload");
        TransferPolicy policy = new TransferPolicy(ConflictPolicy.FAIL, "SHA-256",
                SourceSuccessAction.DELETE, null);

        TransferResult result = execute(prepare(spec(source, target, List.of("item.txt"), List.of(),
                policy, runtime(1)), "run-source-delete"), source, target,
                new InMemoryTransferCheckpointStore());

        assertEquals(TransferRunStatus.SUCCESS, result.status());
        assertFalse(Files.exists(source.resolve("item.txt")));
        assertEquals("payload", Files.readString(target.resolve("item.txt")));
    }

    @Test
    public void cancellationRemovesTemporaryTargetAndCheckpoint() throws Exception {
        Path source = temporaryFolder.newFolder("cancel-source").toPath();
        Path target = temporaryFolder.newFolder("cancel-target").toPath();
        Files.writeString(source.resolve("item.txt"), "abcdefghij");
        PreparedTransfer prepared = prepare(spec(source, target, List.of("item.txt"), List.of(),
                TransferPolicy.defaults(), runtime(1)), "run-cancel");
        TransferPlanItem item = prepared.plan().items().get(0);
        InMemoryTransferCheckpointStore checkpoints = new InMemoryTransferCheckpointStore();
        try (LocalSourcePlugin targetPlugin = connected(target)) {
            TransferWriteSession session = targetPlugin.prepareWrite(item.temporaryPath(), item.itemId());
            assertEquals(4, targetPlugin.append(session, 0,
                    new ByteArrayInputStream("abcd".getBytes(StandardCharsets.UTF_8)), 4));
        }
        checkpoints.save(new TransferCheckpoint(prepared.plan().runId(), item.itemId(),
                item.sourceIdentity(), item.targetIdentity(), item.sourceSnapshot(), item.temporaryPath(),
                4, Instant.now()));
        TransferControl control = new TransferControl();
        control.cancel();

        TransferResult result = new TransferEngine().execute(prepared,
                () -> connected(source), () -> connected(target), checkpoints,
                TransferEventListener.NOOP, control);

        assertEquals(TransferRunStatus.CANCELED, result.status());
        assertEquals(TransferItemStatus.CANCELED, result.items().get(0).status());
        assertFalse(Files.exists(Path.of(item.temporaryPath())));
        assertTrue(checkpoints.load(prepared.plan().runId(), item.itemId()).isEmpty());
        assertFalse(Files.exists(target.resolve("item.txt")));
    }

    @Test
    public void rejectsIllegalStateTransition() {
        TransferItemStateMachine state = new TransferItemStateMachine(TransferItemStatus.QUEUED);

        assertThrows(IllegalStateException.class, () -> state.transitionTo(TransferItemStatus.SUCCESS));
    }

    private PreparedTransfer prepare(TransferSpec spec, String runId) throws Exception {
        try (LocalSourcePlugin source = connected(Path.of(spec.selection().rootPath()))) {
            return new TransferPlanner().prepare(spec, source, runId,
                    Instant.parse("2026-08-07T00:00:00Z"), TransferEventListener.NOOP);
        }
    }

    private TransferResult execute(PreparedTransfer prepared, Path source, Path target,
                                   TransferCheckpointStore checkpoints) throws Exception {
        return new TransferEngine().execute(prepared, () -> connected(source), () -> connected(target),
                checkpoints, TransferEventListener.NOOP, new TransferControl());
    }

    private TransferSpec spec(Path source, Path target, List<String> paths, List<String> includes,
                              TransferPolicy policy, TransferRuntimeOptions runtime) {
        return new TransferSpec(1,
                new TransferEndpoint("local", "source", Map.of("rootPath", source.toString())),
                new TransferEndpoint("local", "target", Map.of("rootPath", target.toString())),
                new TransferSelection(source.toString(), paths, true, includes, null, List.of(),
                        null, null, null, null, 100),
                new TransferMapping(target.toString(), true, null), policy, runtime,
                "UTC", Map.of());
    }

    private TransferRuntimeOptions runtime(int concurrency) {
        return new TransferRuntimeOptions(concurrency, 0, List.of(0L), 64 * 1024, 1_000, 0);
    }

    private LocalSourcePlugin connected(Path root) {
        Configuration configuration = Configuration.newDefault();
        configuration.set("rootPath", root.toString());
        LocalSourcePlugin plugin = new LocalSourcePlugin();
        plugin.connect(configuration);
        return plugin;
    }

    private static final class ObjectKeyFileSystem implements TransferFileSystem {
        private final String key;
        private final List<String> statCalls = new ArrayList<>();

        private ObjectKeyFileSystem(String key) {
            this.key = key;
        }

        @Override
        public TransferFileEntry stat(String path) throws java.io.IOException {
            statCalls.add(path);
            if (!key.equals(path)) {
                throw new java.nio.file.NoSuchFileException(path);
            }
            return new TransferFileEntry(key, "plugin.zip", false, 7L, 1L, "etag");
        }

        @Override
        public TransferFilePage listPage(String directory, String cursor, int pageSize) {
            return new TransferFilePage(List.of(), null, false);
        }

        @Override
        public InputStream openRead(String path, long offset, long length) {
            return new ByteArrayInputStream("payload".getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public TransferWriteSession prepareWrite(String temporaryPath, String sessionId) {
            return new TransferWriteSession(sessionId, temporaryPath, 0L, Map.of());
        }

        @Override
        public long append(TransferWriteSession session, long offset, InputStream source, long length) {
            return offset + length;
        }

        @Override
        public void commit(TransferWriteSession session, String targetPath, boolean replaceExisting) {
        }

        @Override
        public void abort(TransferWriteSession session) {
        }

        @Override
        public void move(String sourcePath, String targetPath, boolean replaceExisting) {
        }

        @Override
        public void delete(String path) {
        }

        @Override
        public StorageCapabilities capabilities() {
            return new StorageCapabilities(true, true, false, false, true, false, Set.of("SHA-256"));
        }

        @Override
        public boolean exists(String path, String name) {
            return key.equals(processingPath(path, name));
        }

        @Override
        public Set<String> listFile(String dir, String regex) {
            return Set.of();
        }

        @Override
        public boolean isFile(String dir, String fileName) {
            return exists(dir, fileName);
        }

        @Override
        public void mkdir(String filePath) {
        }

        @Override
        public void rm(String path) {
        }

        @Override
        public boolean connect(Configuration configuration) {
            return true;
        }

        @Override
        public boolean isConnected() {
            return true;
        }

        @Override
        public boolean mv(String from, String to) {
            return true;
        }

        @Override
        public InputStream getInputStream(String path, String name) {
            return openRead(processingPath(path, name), 0L, 7L);
        }

        @Override
        public OutputStream getOutputStream(String path, String name) {
            return OutputStream.nullOutputStream();
        }

        @Override
        public void close() {
        }
    }
}
