package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.local.LocalSourcePlugin;
import com.jdragon.aggregation.datasource.file.transfer.StorageCapabilities;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.datasource.file.transfer.TransferFilePage;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.datasource.file.transfer.TransferWriteSession;
import com.jdragon.aggregation.transfer.model.ConflictPolicy;
import com.jdragon.aggregation.transfer.model.CheckpointRecoveryMode;
import com.jdragon.aggregation.transfer.model.SourceSuccessAction;
import com.jdragon.aggregation.transfer.model.TransferCheckpoint;
import com.jdragon.aggregation.transfer.model.TransferEndpoint;
import com.jdragon.aggregation.transfer.model.TransferEvent;
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
import java.io.FilterInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
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
    public void streamsSourceChecksumWhenTargetDoesNotExist() throws Exception {
        Path source = temporaryFolder.newFolder("source-before-target-source").toPath();
        Path target = temporaryFolder.newFolder("source-before-target-target").toPath();
        Files.writeString(source.resolve("item.txt"), "payload");
        PreparedTransfer prepared = prepare(spec(source, target, List.of("item.txt"), List.of(),
                TransferPolicy.defaults(), runtime(1)), "run-source-before-target");
        AtomicBoolean targetOpened = new AtomicBoolean();
        AtomicBoolean sourceChecksumCalled = new AtomicBoolean();

        TransferResult result = new TransferEngine().execute(prepared, () -> {
            Configuration configuration = Configuration.newDefault();
            configuration.set("rootPath", source.toString());
            LocalSourcePlugin plugin = new LocalSourcePlugin() {
                @Override
                public String checksum(String path, String algorithm) throws java.io.IOException {
                    sourceChecksumCalled.set(true);
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
        assertFalse("a missing target should not require a source pre-read", sourceChecksumCalled.get());
        assertEquals("payload", Files.readString(target.resolve("item.txt")));
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
        checkpoints.save(resumableCheckpoint(prepared, item,
                "abcd".getBytes(StandardCharsets.UTF_8)));

        List<TransferEvent> events = new ArrayList<TransferEvent>();
        TransferResult result = new TransferEngine().execute(prepared,
                () -> connected(source), () -> connected(target), checkpoints,
                events::add, new TransferControl());

        assertEquals(TransferRunStatus.SUCCESS, result.status());
        assertEquals(4, result.items().get(0).resumedBytes());
        TransferEvent resumed = events.stream()
                .filter(event -> event.itemStatus() == TransferItemStatus.TRANSFERRING)
                .findFirst().orElseThrow();
        assertEquals(4L, resumed.transferredBytes());
        assertEquals(4L, resumed.details().get("resumedBytes"));
        assertEquals("RESUMING_TRANSFER", resumed.details().get("resumePhase"));
        assertEquals("checkpoint restored; transfer resumes from confirmed offset", resumed.message());
        assertEquals("abcdefghij", Files.readString(target.resolve("item.txt")));
        assertTrue(checkpoints.load(prepared.plan().runId(), item.itemId()).isEmpty());
    }

    @Test
    public void fastResumeDoesNotReadTheConfirmedSourcePrefix() throws Exception {
        Path source = temporaryFolder.newFolder("fast-resume-source").toPath();
        Path target = temporaryFolder.newFolder("fast-resume-target").toPath();
        byte[] content = "abcdefghij".getBytes(StandardCharsets.UTF_8);
        Files.write(source.resolve("item.txt"), content);
        PreparedTransfer prepared = prepare(spec(source, target, List.of("item.txt"), List.of(),
                TransferPolicy.defaults(), runtime(1)), "run-fast-resume");
        TransferPlanItem item = prepared.plan().items().get(0);
        InMemoryTransferCheckpointStore checkpoints = new InMemoryTransferCheckpointStore();
        try (LocalSourcePlugin targetPlugin = connected(target)) {
            TransferWriteSession session = targetPlugin.prepareWrite(item.temporaryPath(), item.itemId());
            assertEquals(4, targetPlugin.append(session, 0,
                    new ByteArrayInputStream(content, 0, 4), 4));
        }
        checkpoints.save(resumableCheckpoint(prepared, item,
                java.util.Arrays.copyOfRange(content, 0, 4)));
        List<Long> offsets = new java.util.concurrent.CopyOnWriteArrayList<Long>();

        TransferResult result = new TransferEngine().execute(prepared,
                () -> trackingSource(source, offsets), () -> connected(target), checkpoints,
                TransferEventListener.NOOP, new TransferControl());

        assertEquals(TransferRunStatus.SUCCESS, result.status());
        assertEquals(List.of(0L, 4L), offsets);
        assertEquals("abcdefghij", Files.readString(target.resolve("item.txt")));
    }

    @Test
    public void fastResumeDiscardsOnlyTheUnconfirmedTemporaryTail() throws Exception {
        Path source = temporaryFolder.newFolder("tail-resume-source").toPath();
        Path target = temporaryFolder.newFolder("tail-resume-target").toPath();
        byte[] content = "abcdefghij".getBytes(StandardCharsets.UTF_8);
        Files.write(source.resolve("item.txt"), content);
        PreparedTransfer prepared = prepare(spec(source, target, List.of("item.txt"), List.of(),
                TransferPolicy.defaults(), runtime(1)), "run-tail-resume");
        TransferPlanItem item = prepared.plan().items().get(0);
        InMemoryTransferCheckpointStore checkpoints = new InMemoryTransferCheckpointStore();
        try (LocalSourcePlugin targetPlugin = connected(target)) {
            TransferWriteSession session = targetPlugin.prepareWrite(item.temporaryPath(), item.itemId());
            assertEquals(6, targetPlugin.append(session, 0,
                    new ByteArrayInputStream(content, 0, 6), 6));
        }
        checkpoints.save(resumableCheckpoint(prepared, item,
                java.util.Arrays.copyOfRange(content, 0, 4)));
        List<TransferEvent> events = new java.util.concurrent.CopyOnWriteArrayList<TransferEvent>();

        TransferResult result = new TransferEngine().execute(prepared,
                () -> connected(source), () -> connected(target), checkpoints,
                events::add, new TransferControl());

        assertEquals(TransferRunStatus.SUCCESS, result.status());
        assertEquals(4L, result.items().get(0).resumedBytes());
        assertTrue(events.stream().anyMatch(event -> "RESUMING_TRANSFER".equals(
                event.details().get("resumePhase"))));
        assertEquals("abcdefghij", Files.readString(target.resolve("item.txt")));
    }

    @Test
    public void fastModeRestartsLegacyCheckpointWithoutReadingItsPrefix() throws Exception {
        Path source = temporaryFolder.newFolder("legacy-fast-source").toPath();
        Path target = temporaryFolder.newFolder("legacy-fast-target").toPath();
        byte[] content = "abcdefghij".getBytes(StandardCharsets.UTF_8);
        Files.write(source.resolve("item.txt"), content);
        PreparedTransfer prepared = prepare(spec(source, target, List.of("item.txt"), List.of(),
                TransferPolicy.defaults(), runtime(1)), "run-legacy-fast");
        TransferPlanItem item = prepared.plan().items().get(0);
        InMemoryTransferCheckpointStore checkpoints = new InMemoryTransferCheckpointStore();
        try (LocalSourcePlugin targetPlugin = connected(target)) {
            TransferWriteSession session = targetPlugin.prepareWrite(item.temporaryPath(), item.itemId());
            assertEquals(4, targetPlugin.append(session, 0,
                    new ByteArrayInputStream(content, 0, 4), 4));
        }
        checkpoints.save(new TransferCheckpoint(prepared.plan().runId(), item.itemId(),
                item.sourceIdentity(), item.targetIdentity(), item.sourceSnapshot(), item.temporaryPath(),
                4, Instant.now()));
        List<TransferEvent> events = new java.util.concurrent.CopyOnWriteArrayList<TransferEvent>();

        TransferResult result = new TransferEngine().execute(prepared,
                () -> connected(source), () -> connected(target), checkpoints,
                events::add, new TransferControl());

        assertEquals(TransferRunStatus.SUCCESS, result.status());
        assertEquals(0L, result.items().get(0).resumedBytes());
        TransferEvent restarted = events.stream()
                .filter(event -> "RESTARTED_FROM_ZERO".equals(event.details().get("resumePhase")))
                .findFirst().orElseThrow();
        assertTrue(restarted.message().contains("legacy checkpoint"));
        assertEquals("abcdefghij", Files.readString(target.resolve("item.txt")));
    }

    @Test
    public void fastModeRestartsWhenCheckpointBoundaryFingerprintChanges() throws Exception {
        Path source = temporaryFolder.newFolder("boundary-source").toPath();
        Path target = temporaryFolder.newFolder("boundary-target").toPath();
        byte[] content = "abcdefghij".getBytes(StandardCharsets.UTF_8);
        Files.write(source.resolve("item.txt"), content);
        PreparedTransfer prepared = prepare(spec(source, target, List.of("item.txt"), List.of(),
                TransferPolicy.defaults(), runtime(1)), "run-boundary-restart");
        TransferPlanItem item = prepared.plan().items().get(0);
        InMemoryTransferCheckpointStore checkpoints = new InMemoryTransferCheckpointStore();
        try (LocalSourcePlugin targetPlugin = connected(target)) {
            TransferWriteSession session = targetPlugin.prepareWrite(item.temporaryPath(), item.itemId());
            assertEquals(4, targetPlugin.append(session, 0,
                    new ByteArrayInputStream(content, 0, 4), 4));
        }
        TransferCheckpoint valid = resumableCheckpoint(prepared, item,
                java.util.Arrays.copyOfRange(content, 0, 4));
        checkpoints.save(new TransferCheckpoint(valid.runId(), valid.itemId(), valid.sourceIdentity(),
                valid.targetIdentity(), valid.sourceSnapshot(), valid.temporaryPath(), valid.confirmedOffset(),
                valid.schemaVersion(), valid.checksumAlgorithm(), valid.checksumStateFormat(),
                valid.checksumStateBase64(),
                valid.checksumStateOffset(), valid.sourceChecksum(), valid.headOffset(),
                valid.headLength(), valid.headSha256(), valid.boundaryOffset(),
                valid.boundaryLength(), "00", valid.updatedAt()));

        TransferResult result = execute(prepared, source, target, checkpoints);

        assertEquals(TransferRunStatus.SUCCESS, result.status());
        assertEquals(0L, result.items().get(0).resumedBytes());
        assertEquals("abcdefghij", Files.readString(target.resolve("item.txt")));
    }

    @Test
    public void restartsWhenTemporaryTargetDoesNotMatchCheckpoint() throws Exception {
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

        assertEquals(TransferRunStatus.SUCCESS, result.status());
        assertEquals("abcdefghij", Files.readString(target.resolve("item.txt")));
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

    @Test
    public void publishesLiveProgressBeforeTheCurrentChunkIsCheckpointed() throws Exception {
        Path source = temporaryFolder.newFolder("live-progress-source").toPath();
        Path target = temporaryFolder.newFolder("live-progress-target").toPath();
        byte[] content = new byte[64 * 1024];
        Files.write(source.resolve("slow.bin"), content);
        TransferRuntimeOptions runtime = new TransferRuntimeOptions(
                1, 0, List.of(0L), 64 * 1024, 5L, 0L,
                CheckpointRecoveryMode.STRICT);
        PreparedTransfer prepared = prepare(spec(source, target, List.of("slow.bin"), List.of(),
                TransferPolicy.defaults(), runtime), "run-live-progress");
        List<TransferEvent> events = new java.util.concurrent.CopyOnWriteArrayList<TransferEvent>();

        TransferResult result = new TransferEngine().execute(prepared,
                () -> connected(source), () -> slowTarget(target), new InMemoryTransferCheckpointStore(),
                events::add, new TransferControl());

        assertEquals(TransferRunStatus.SUCCESS, result.status());
        int liveIndex = indexOf(events, event -> Boolean.TRUE.equals(event.details().get("live")));
        int checkpointIndex = indexOf(events, event -> event.type()
                == com.jdragon.aggregation.transfer.model.TransferEventType.CHECKPOINT_SAVED);
        assertTrue("a live event must be emitted before the 64 KiB chunk is confirmed", liveIndex >= 0);
        assertTrue("live progress must precede the checkpoint event", liveIndex < checkpointIndex);
        TransferEvent live = events.get(liveIndex);
        assertEquals(0L, live.transferredBytes());
        assertEquals(0L, live.details().get("confirmedBytes"));
        assertTrue(((Number) live.details().get("observedBytes")).longValue() > 0L);
    }

    @Test
    public void publishesChecksumRebuildActivityWithoutAdvancingConfirmedProgress() throws Exception {
        Path source = temporaryFolder.newFolder("resume-activity-source").toPath();
        Path target = temporaryFolder.newFolder("resume-activity-target").toPath();
        byte[] content = new byte[64 * 1024];
        Files.write(source.resolve("slow.bin"), content);
        TransferRuntimeOptions runtime = new TransferRuntimeOptions(
                1, 0, List.of(0L), 64 * 1024, 5L, 0L,
                CheckpointRecoveryMode.STRICT);
        PreparedTransfer prepared = prepare(spec(source, target, List.of("slow.bin"), List.of(),
                TransferPolicy.defaults(), runtime), "run-resume-activity");
        TransferPlanItem item = prepared.plan().items().get(0);
        InMemoryTransferCheckpointStore checkpoints = new InMemoryTransferCheckpointStore();
        long confirmedOffset = 32 * 1024L;
        try (LocalSourcePlugin targetPlugin = connected(target)) {
            TransferWriteSession session = targetPlugin.prepareWrite(item.temporaryPath(), item.itemId());
            assertEquals(confirmedOffset, targetPlugin.append(session, 0,
                    new ByteArrayInputStream(content, 0, (int) confirmedOffset), confirmedOffset));
        }
        checkpoints.save(new TransferCheckpoint(prepared.plan().runId(), item.itemId(),
                item.sourceIdentity(), item.targetIdentity(), item.sourceSnapshot(), item.temporaryPath(),
                confirmedOffset, Instant.now()));
        List<TransferEvent> events = new java.util.concurrent.CopyOnWriteArrayList<TransferEvent>();

        TransferResult result = new TransferEngine().execute(prepared,
                () -> slowSource(source), () -> connected(target), checkpoints,
                events::add, new TransferControl());

        assertEquals(TransferRunStatus.SUCCESS, result.status());
        TransferEvent rebuilding = events.stream()
                .filter(event -> Boolean.TRUE.equals(event.details().get("live")))
                .filter(event -> "REBUILDING_CHECKSUM".equals(event.details().get("resumePhase")))
                .findFirst().orElseThrow();
        assertEquals(confirmedOffset, rebuilding.transferredBytes());
        assertEquals(confirmedOffset, rebuilding.details().get("confirmedBytes"));
        assertEquals(confirmedOffset, rebuilding.details().get("observedBytes"));
        assertTrue(((Number) rebuilding.details().get("activityBytes")).longValue() > confirmedOffset);
        TransferEvent completedBlock = events.stream()
                .filter(event -> event.type()
                        == com.jdragon.aggregation.transfer.model.TransferEventType.ITEM_PROGRESS)
                .filter(event -> !Boolean.TRUE.equals(event.details().get("live")))
                .filter(event -> event.details().containsKey("resumedBytes"))
                .findFirst().orElseThrow();
        assertEquals(64 * 1024L, completedBlock.transferredBytes());
        assertEquals(96 * 1024L, completedBlock.details().get("activityBytes"));
    }

    @Test
    public void cancellationDuringChecksumRebuildIsReportedAsCanceled() throws Exception {
        Path source = temporaryFolder.newFolder("cancel-resume-source").toPath();
        Path target = temporaryFolder.newFolder("cancel-resume-target").toPath();
        byte[] content = new byte[256 * 1024];
        Files.write(source.resolve("slow.bin"), content);
        TransferRuntimeOptions runtime = new TransferRuntimeOptions(
                1, 0, List.of(0L), 64 * 1024, 5L, 0L,
                CheckpointRecoveryMode.STRICT);
        PreparedTransfer prepared = prepare(spec(source, target, List.of("slow.bin"), List.of(),
                TransferPolicy.defaults(), runtime), "run-cancel-resume");
        TransferPlanItem item = prepared.plan().items().get(0);
        InMemoryTransferCheckpointStore checkpoints = new InMemoryTransferCheckpointStore();
        long confirmedOffset = 128 * 1024L;
        try (LocalSourcePlugin targetPlugin = connected(target)) {
            TransferWriteSession session = targetPlugin.prepareWrite(item.temporaryPath(), item.itemId());
            assertEquals(confirmedOffset, targetPlugin.append(session, 0,
                    new ByteArrayInputStream(content, 0, (int) confirmedOffset), confirmedOffset));
        }
        checkpoints.save(new TransferCheckpoint(prepared.plan().runId(), item.itemId(),
                item.sourceIdentity(), item.targetIdentity(), item.sourceSnapshot(), item.temporaryPath(),
                confirmedOffset, Instant.now()));
        TransferControl control = new TransferControl();
        List<TransferEvent> events = new java.util.concurrent.CopyOnWriteArrayList<TransferEvent>();

        TransferResult result = new TransferEngine().execute(prepared,
                () -> slowSource(source), () -> connected(target), checkpoints,
                event -> {
                    events.add(event);
                    if (Boolean.TRUE.equals(event.details().get("live"))
                            && "REBUILDING_CHECKSUM".equals(event.details().get("resumePhase"))) {
                        control.cancel();
                    }
                }, control);

        assertEquals(TransferRunStatus.CANCELED, result.status());
        assertEquals(TransferItemStatus.CANCELED, result.items().get(0).status());
        assertTrue(checkpoints.load(prepared.plan().runId(), item.itemId()).isEmpty());
        assertFalse(Files.exists(target.resolve("slow.bin")));
    }

    @Test
    public void resumedTransferWithExistingConflictingTargetKeepsActivityMonotonic() throws Exception {
        Path source = temporaryFolder.newFolder("resume-conflict-source").toPath();
        Path target = temporaryFolder.newFolder("resume-conflict-target").toPath();
        byte[] content = new byte[128 * 1024];
        java.util.Arrays.fill(content, (byte) 's');
        Files.write(source.resolve("item.bin"), content);
        Files.write(target.resolve("item.bin"), "different-target".getBytes(StandardCharsets.UTF_8));
        TransferRuntimeOptions runtime = new TransferRuntimeOptions(
                1, 0, List.of(0L), 64 * 1024, 5L, 0L);
        TransferPolicy policy = new TransferPolicy(ConflictPolicy.OVERWRITE, "SHA-256",
                SourceSuccessAction.KEEP, null);
        PreparedTransfer prepared = prepare(spec(source, target, List.of("item.bin"), List.of(),
                policy, runtime), "run-resume-conflict");
        TransferPlanItem item = prepared.plan().items().get(0);
        InMemoryTransferCheckpointStore checkpoints = new InMemoryTransferCheckpointStore();
        long confirmedOffset = 64 * 1024L;
        try (LocalSourcePlugin targetPlugin = connected(target)) {
            TransferWriteSession session = targetPlugin.prepareWrite(item.temporaryPath(), item.itemId());
            assertEquals(confirmedOffset, targetPlugin.append(session, 0,
                    new ByteArrayInputStream(content, 0, (int) confirmedOffset), confirmedOffset));
        }
        checkpoints.save(new TransferCheckpoint(prepared.plan().runId(), item.itemId(),
                item.sourceIdentity(), item.targetIdentity(), item.sourceSnapshot(), item.temporaryPath(),
                confirmedOffset, Instant.now()));
        List<TransferEvent> events = new java.util.concurrent.CopyOnWriteArrayList<TransferEvent>();

        TransferResult result = new TransferEngine().execute(prepared,
                () -> slowSource(source), () -> slowTarget(target), checkpoints,
                events::add, new TransferControl());

        assertEquals(TransferRunStatus.SUCCESS, result.status());
        List<TransferEvent> progress = events.stream()
                .filter(event -> event.type()
                        == com.jdragon.aggregation.transfer.model.TransferEventType.ITEM_PROGRESS)
                .filter(event -> event.details().containsKey("activityBytes"))
                .toList();
        assertTrue(progress.size() >= 2);
        for (int i = 1; i < progress.size(); i++) {
            long previous = ((Number) progress.get(i - 1).details().get("activityBytes")).longValue();
            long current = ((Number) progress.get(i).details().get("activityBytes")).longValue();
            assertTrue("activity bytes must not move backwards", current >= previous);
        }
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

    private TransferCheckpoint resumableCheckpoint(PreparedTransfer prepared,
                                                   TransferPlanItem item,
                                                   byte[] confirmedBytes) throws Exception {
        ResumableSha256 digest = ResumableSha256.create();
        digest.update(confirmedBytes, 0, confirmedBytes.length);
        MessageDigest boundary = MessageDigest.getInstance("SHA-256");
        boundary.update(confirmedBytes);
        return new TransferCheckpoint(prepared.plan().runId(), item.itemId(),
                item.sourceIdentity(), item.targetIdentity(), item.sourceSnapshot(), item.temporaryPath(),
                confirmedBytes.length, TransferCheckpoint.CURRENT_SCHEMA_VERSION, "SHA-256",
                "BC-SHA256-ENCODED-V1", digest.encodedState(), confirmedBytes.length, null,
                0L, confirmedBytes.length, HexFormat.of().formatHex(boundary.digest()),
                0L, confirmedBytes.length,
                sha256(confirmedBytes), Instant.now());
    }

    private String sha256(byte[] value) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(digest.digest(value));
    }

    private LocalSourcePlugin trackingSource(Path root, List<Long> offsets) {
        Configuration configuration = Configuration.newDefault();
        configuration.set("rootPath", root.toString());
        LocalSourcePlugin plugin = new LocalSourcePlugin() {
            @Override
            public InputStream openRead(String path, long offset, long length) throws java.io.IOException {
                offsets.add(offset);
                return super.openRead(path, offset, length);
            }
        };
        plugin.connect(configuration);
        return plugin;
    }

    private LocalSourcePlugin connected(Path root) {
        Configuration configuration = Configuration.newDefault();
        configuration.set("rootPath", root.toString());
        LocalSourcePlugin plugin = new LocalSourcePlugin();
        plugin.connect(configuration);
        return plugin;
    }

    private LocalSourcePlugin slowTarget(Path root) {
        Configuration configuration = Configuration.newDefault();
        configuration.set("rootPath", root.toString());
        LocalSourcePlugin plugin = new LocalSourcePlugin() {
            @Override
            public long append(TransferWriteSession session, long offset, InputStream source, long length)
                    throws java.io.IOException {
                InputStream slow = new FilterInputStream(source) {
                    @Override
                    public int read(byte[] buffer, int bufferOffset, int requested) throws java.io.IOException {
                        try {
                            Thread.sleep(6L);
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                            throw new java.io.InterruptedIOException("slow target interrupted");
                        }
                        return super.read(buffer, bufferOffset, Math.min(requested, 4 * 1024));
                    }
                };
                return super.append(session, offset, slow, length);
            }
        };
        plugin.connect(configuration);
        return plugin;
    }

    private LocalSourcePlugin slowSource(Path root) {
        Configuration configuration = Configuration.newDefault();
        configuration.set("rootPath", root.toString());
        LocalSourcePlugin plugin = new LocalSourcePlugin() {
            @Override
            public InputStream openRead(String path, long offset, long length) throws java.io.IOException {
                return new FilterInputStream(super.openRead(path, offset, length)) {
                    @Override
                    public int read(byte[] buffer, int bufferOffset, int requested) throws java.io.IOException {
                        try {
                            Thread.sleep(6L);
                        } catch (InterruptedException exception) {
                            Thread.currentThread().interrupt();
                            throw new java.io.InterruptedIOException("slow source interrupted");
                        }
                        return super.read(buffer, bufferOffset, Math.min(requested, 4 * 1024));
                    }
                };
            }
        };
        plugin.connect(configuration);
        return plugin;
    }

    private int indexOf(List<TransferEvent> events,
                        java.util.function.Predicate<TransferEvent> predicate) {
        for (int index = 0; index < events.size(); index++) {
            if (predicate.test(events.get(index))) {
                return index;
            }
        }
        return -1;
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
