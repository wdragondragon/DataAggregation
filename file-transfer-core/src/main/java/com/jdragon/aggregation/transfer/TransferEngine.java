package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.datasource.file.transfer.TransferWriteSession;
import com.jdragon.aggregation.transfer.model.ConflictPolicy;
import com.jdragon.aggregation.transfer.model.CheckpointRecoveryMode;
import com.jdragon.aggregation.transfer.model.SourceSnapshot;
import com.jdragon.aggregation.transfer.model.SourceSuccessAction;
import com.jdragon.aggregation.transfer.model.TransferCheckpoint;
import com.jdragon.aggregation.transfer.model.TransferEvent;
import com.jdragon.aggregation.transfer.model.TransferEventType;
import com.jdragon.aggregation.transfer.model.TransferItemResult;
import com.jdragon.aggregation.transfer.model.TransferItemStatus;
import com.jdragon.aggregation.transfer.model.TransferPlan;
import com.jdragon.aggregation.transfer.model.TransferPlanItem;
import com.jdragon.aggregation.transfer.model.TransferPolicy;
import com.jdragon.aggregation.transfer.model.TransferResult;
import com.jdragon.aggregation.transfer.model.TransferRunStatus;
import com.jdragon.aggregation.transfer.model.TransferRuntimeOptions;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;

public class TransferEngine {

    private final UnaryOperator<Runnable> taskDecorator;

    public TransferEngine() {
        this(UnaryOperator.identity());
    }

    public TransferEngine(UnaryOperator<Runnable> taskDecorator) {
        this.taskDecorator = taskDecorator == null ? UnaryOperator.identity() : taskDecorator;
    }

    public TransferResult execute(PreparedTransfer prepared,
                                  TransferFileSystemFactory sourceFactory,
                                  TransferFileSystemFactory targetFactory,
                                  TransferCheckpointStore checkpointStore,
                                  TransferEventListener listener,
                                  TransferControl control) throws InterruptedException {
        TransferPlan plan = prepared.plan();
        TransferPolicy policy = prepared.resolvedSpec().policy();
        TransferRuntimeOptions runtime = prepared.resolvedSpec().runtime();
        TransferCheckpointStore checkpoints = checkpointStore == null
                ? new InMemoryTransferCheckpointStore()
                : checkpointStore;
        TransferEventListener events = listener == null ? TransferEventListener.NOOP : listener;
        TransferControl effectiveControl = control == null ? new TransferControl() : control;
        Instant startedAt = Instant.now();
        events.onEvent(event(TransferEventType.RUN_STARTED, plan.runId(), null, null,
                0, totalBytes(plan), 0, "file transfer run started", null,
                Map.of("fileCount", plan.items().size(), "concurrency", runtime.concurrency())));

        AtomicInteger threadSequence = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(runtime.concurrency(), runnable -> {
            Thread thread = new Thread(runnable,
                    "file-transfer-" + plan.runId() + "-" + threadSequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        });
        List<CompletableFuture<TransferItemResult>> futures = new ArrayList<>();
        try {
            for (TransferPlanItem item : plan.items()) {
                CompletableFuture<TransferItemResult> future = new CompletableFuture<>();
                Runnable task = () -> {
                    try {
                        future.complete(executeWithRetry(plan, item, policy, runtime, sourceFactory,
                                targetFactory, checkpoints, events, effectiveControl));
                    } catch (Throwable error) {
                        future.completeExceptionally(error);
                    }
                };
                executor.execute(taskDecorator.apply(task));
                futures.add(future);
            }
            List<TransferItemResult> items = new ArrayList<>();
            for (CompletableFuture<TransferItemResult> future : futures) {
                try {
                    items.add(future.get());
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause() == null ? e : e.getCause();
                    throw new IllegalStateException("file transfer worker failed", cause);
                }
            }
            items.sort(Comparator.comparing(TransferItemResult::itemId));
            TransferResult result = result(plan, items, startedAt, Instant.now());
            events.onEvent(event(TransferEventType.RUN_COMPLETED, plan.runId(), null, null,
                    result.transferredBytes(), result.totalBytes(), 0,
                    "file transfer run completed", null,
                    Map.of("status", result.status().name(), "successFiles", result.successFiles(),
                            "skippedFiles", result.skippedFiles(), "failedFiles", result.failedFiles())));
            return result;
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    private TransferItemResult executeWithRetry(TransferPlan plan, TransferPlanItem item,
                                                TransferPolicy policy, TransferRuntimeOptions runtime,
                                                TransferFileSystemFactory sourceFactory,
                                                TransferFileSystemFactory targetFactory,
                                                TransferCheckpointStore checkpoints,
                                                TransferEventListener events,
                                                TransferControl control) {
        Instant startedAt = Instant.now();
        TransferItemStateMachine state = new TransferItemStateMachine(TransferItemStatus.QUEUED);
        int attempts = 0;
        boolean integrityRestarted = false;
        while (true) {
            attempts++;
            try {
                if (control.isCanceled()) {
                    cleanupCanceled(plan, item, targetFactory, checkpoints);
                    transition(plan, item, state, TransferItemStatus.CANCELED, 0, attempts, events,
                            "transfer canceled before start", null);
                    return failureResult(item, state.status(), attempts, "CANCELED",
                            "transfer was canceled", startedAt);
                }
                return executeAttempt(plan, item, policy, runtime, sourceFactory, targetFactory,
                        checkpoints, events, control, state, attempts, startedAt);
            } catch (TransferCanceledException e) {
                cleanupCanceled(plan, item, targetFactory, checkpoints);
                transition(plan, item, state, TransferItemStatus.CANCELED, 0, attempts, events,
                        e.getMessage(), "CANCELED");
                return failureResult(item, TransferItemStatus.CANCELED, attempts,
                        "CANCELED", e.getMessage(), startedAt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                transition(plan, item, state, TransferItemStatus.CANCELED, 0, attempts, events,
                        "transfer thread interrupted", "INTERRUPTED");
                return failureResult(item, TransferItemStatus.CANCELED, attempts,
                        "INTERRUPTED", e.getMessage(), startedAt);
            } catch (Exception e) {
                // Checkpoint checksum reconstruction reports through an InputStream callback,
                // whose contract only permits IOException. Preserve the cancellation cause so
                // a cancel during reconstruction is not misclassified as an ordinary I/O failure.
                if (isCanceled(e)) {
                    cleanupCanceled(plan, item, targetFactory, checkpoints);
                    transition(plan, item, state, TransferItemStatus.CANCELED, 0, attempts, events,
                            "transfer was canceled", "CANCELED");
                    return failureResult(item, TransferItemStatus.CANCELED, attempts,
                            "CANCELED", "transfer was canceled", startedAt);
                }
                if (e instanceof RestartFromZeroException && !integrityRestarted) {
                    integrityRestarted = true;
                    transition(plan, item, state, TransferItemStatus.FAILED, 0, attempts, events,
                            safeMessage(e), "CHECKSUM_MISMATCH");
                    events.onEvent(event(TransferEventType.ITEM_RETRYING, plan.runId(), item.itemId(),
                            TransferItemStatus.FAILED, 0, item.sourceSnapshot().size(), attempts,
                            "checksum validation failed; retrying once from zero",
                            "CHECKSUM_MISMATCH", Map.of("restartFromZero", true)));
                    transition(plan, item, state, TransferItemStatus.QUEUED, 0, attempts, events,
                            "file queued for one integrity retry from zero", null);
                    continue;
                }
                String errorCode = errorCode(e);
                transition(plan, item, state, TransferItemStatus.FAILED, 0, attempts, events,
                        safeMessage(e), errorCode);
                if (e instanceof RestartFromZeroException
                        || attempts > runtime.maxRetries() || !retryable(e)) {
                    return failureResult(item, TransferItemStatus.FAILED, attempts,
                            errorCode, safeMessage(e), startedAt);
                }
                long delay = runtime.retryDelayMillis(attempts);
                events.onEvent(event(TransferEventType.ITEM_RETRYING, plan.runId(), item.itemId(),
                        TransferItemStatus.FAILED, 0, item.sourceSnapshot().size(), attempts,
                        "retrying file transfer after " + delay + " ms", errorCode,
                        Map.of("delayMillis", delay)));
                try {
                    sleepWithControl(delay, control);
                } catch (TransferCanceledException canceled) {
                    cleanupCanceled(plan, item, targetFactory, checkpoints);
                    transition(plan, item, state, TransferItemStatus.CANCELED, 0, attempts, events,
                            canceled.getMessage(), "CANCELED");
                    return failureResult(item, TransferItemStatus.CANCELED, attempts,
                            "CANCELED", canceled.getMessage(), startedAt);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    transition(plan, item, state, TransferItemStatus.CANCELED, 0, attempts, events,
                            "transfer thread interrupted", "INTERRUPTED");
                    return failureResult(item, TransferItemStatus.CANCELED, attempts,
                            "INTERRUPTED", interrupted.getMessage(), startedAt);
                }
                transition(plan, item, state, TransferItemStatus.QUEUED, 0, attempts, events,
                        "file queued for retry", null);
            }
        }
    }

    private void cleanupCanceled(TransferPlan plan, TransferPlanItem item,
                                 TransferFileSystemFactory targetFactory,
                                 TransferCheckpointStore checkpoints) {
        try (TransferFileSystem target = targetFactory.open()) {
            TransferWriteSession session = target.prepareWrite(item.temporaryPath(), item.itemId());
            target.abort(session);
            checkpoints.delete(plan.runId(), item.itemId());
        } catch (Exception ignored) {
            // Cancellation remains authoritative; stale temporary data can be reclaimed by retention cleanup.
        }
    }

    private TransferItemResult executeAttempt(TransferPlan plan, TransferPlanItem item,
                                              TransferPolicy policy, TransferRuntimeOptions runtime,
                                              TransferFileSystemFactory sourceFactory,
                                              TransferFileSystemFactory targetFactory,
                                              TransferCheckpointStore checkpoints,
                                              TransferEventListener events, TransferControl control,
                                              TransferItemStateMachine state, int attempt,
                                              Instant startedAt) throws Exception {
        try (TransferFileSystem source = sourceFactory.open()) {
            TransferFileEntry currentSource = source.stat(item.sourcePath());
            ensureSourceSnapshot(item.sourceSnapshot(), currentSource);

            try (TransferFileSystem target = targetFactory.open()) {
            boolean targetExisted = target.transferExists(item.targetPath());
            String sourceChecksum = null;
            String originalTargetChecksum = null;
            if (targetExisted) {
                sourceChecksum = source.checksum(item.sourcePath(), policy.checksumAlgorithm());
                originalTargetChecksum = target.checksum(item.targetPath(), policy.checksumAlgorithm());
                if (sourceChecksum.equals(originalTargetChecksum)) {
                    checkpoints.delete(plan.runId(), item.itemId());
                    transition(plan, item, state, TransferItemStatus.SKIPPED, 0, attempt, events,
                            "target already has the same SHA-256", null);
                    return successResult(item, TransferItemStatus.SKIPPED, 0, 0, attempt,
                            sourceChecksum, originalTargetChecksum, startedAt);
                }
                if (policy.conflictPolicy() == ConflictPolicy.FAIL) {
                    transition(plan, item, state, TransferItemStatus.CONFLICT, 0, attempt, events,
                            "target exists with different content", "TARGET_CONFLICT");
                    return failureResult(item, TransferItemStatus.CONFLICT, attempt,
                            "TARGET_CONFLICT", "target exists with different content", startedAt);
                }
            }

            ResumeSession resume = prepareSession(plan, item, source, target, checkpoints,
                    runtime.checkpointRecoveryMode(), sourceChecksum != null);
            long offset = resume.session().confirmedOffset();
            long resumedBytes = offset;
            // Validate the persisted checkpoint before publishing the initial
            // transfer state, so a resumed item never visibly falls back to zero.
            enterTransferring(plan, item, state, control, events, attempt, offset,
                    resume.rebuildingChecksum(), resume.restartReason());
            long attemptStartedNanos = System.nanoTime();
            ResumableSha256 streamingDigest = sourceChecksum == null
                    ? resume.checksum() == null ? ResumableSha256.create() : resume.checksum()
                    : null;
            byte[] headFingerprint = resume.headFingerprint();
            // Activity is monotonic across a resumed attempt even when the source checksum
            // was already calculated because the target existed and conflicted.
            long activityBaseBytes = offset;
            if (streamingDigest != null && offset > 0 && resume.rebuildingChecksum()) {
                long confirmedResumeOffset = offset;
                final long prefixActivityBase = confirmedResumeOffset;
                try (InputStream prefix = source.openRead(item.sourcePath(), 0, confirmedResumeOffset)) {
                    // The checksum rebuild reads the retained prefix, but it does not advance the
                    // confirmed target offset. Activity includes the retained bytes plus this read.
                    InputStream observedPrefix = new LiveProgressInputStream(prefix, 0L,
                            runtime.checkpointIntervalMillis(), activityBytes ->
                            publishChecksumRebuildProgress(plan, item, state, control, events, attempt,
                                    confirmedResumeOffset, resumedBytes,
                                    prefixActivityBase + activityBytes));
                    updateDigest(observedPrefix, confirmedResumeOffset, streamingDigest, item.sourcePath());
                }
                activityBaseBytes = confirmedResumeOffset * 2L;
            }
            while (offset < item.sourceSnapshot().size()) {
                offset = pauseOrCancel(plan, item, state, control, events, attempt, offset);
                long length = Math.min(runtime.chunkSizeBytes(), item.sourceSnapshot().size() - offset);
                long written;
                CheckpointDigestInputStream digestInput = null;
                try (InputStream input = source.openRead(item.sourcePath(), offset, length)) {
                    digestInput = streamingDigest == null
                            ? null
                            : new CheckpointDigestInputStream(input, streamingDigest);
                    InputStream transferInput = digestInput == null ? input : digestInput;
                    long confirmedOffset = offset;
                    final long chunkActivityBase = activityBaseBytes;
                    transferInput = new LiveProgressInputStream(transferInput, confirmedOffset,
                            runtime.checkpointIntervalMillis(), observedBytes ->
                            events.onEvent(event(TransferEventType.ITEM_PROGRESS, plan.runId(), item.itemId(),
                                    TransferItemStatus.TRANSFERRING, confirmedOffset,
                                    item.sourceSnapshot().size(), attempt,
                                    "file block transfer in progress", null,
                                    Map.of("live", true,
                                            "confirmedBytes", confirmedOffset,
                                            "observedBytes", observedBytes,
                                            "activityBytes", chunkActivityBase + observedBytes - confirmedOffset,
                                            "resumedBytes", resumedBytes))));
                    written = target.append(resume.session(), offset, transferInput, length);
                }
                if (written != length) {
                    throw new IOException("unexpected EOF: expected " + length + " bytes but wrote " + written);
                }
                offset += written;
                activityBaseBytes += written;
                if (headFingerprint == null && digestInput != null) {
                    headFingerprint = digestInput.headBytes();
                }
                TransferCheckpoint checkpoint = new TransferCheckpoint(plan.runId(), item.itemId(),
                        item.sourceIdentity(), item.targetIdentity(), item.sourceSnapshot(),
                        item.temporaryPath(), offset, TransferCheckpoint.CURRENT_SCHEMA_VERSION,
                        policy.checksumAlgorithm(),
                        "BC-SHA256-ENCODED-V1",
                        streamingDigest == null ? null : streamingDigest.encodedState(),
                        streamingDigest == null ? 0L : offset,
                        sourceChecksum,
                        0L,
                        headFingerprint == null ? 0 : headFingerprint.length,
                        headFingerprint == null ? null : sha256(headFingerprint),
                        digestInput == null ? 0L : offset - digestInput.tailLength(),
                        digestInput == null ? 0 : digestInput.tailLength(),
                        digestInput == null ? null : digestInput.tailSha256(),
                        Instant.now());
                checkpoints.save(checkpoint);
                events.onEvent(event(TransferEventType.CHECKPOINT_SAVED, plan.runId(), item.itemId(),
                        TransferItemStatus.TRANSFERRING, offset, item.sourceSnapshot().size(), attempt,
                        "transfer checkpoint saved", null,
                        Map.of("activityBytes", activityBaseBytes,
                                "resumedBytes", resumedBytes)));
                events.onEvent(event(TransferEventType.ITEM_PROGRESS, plan.runId(), item.itemId(),
                        TransferItemStatus.TRANSFERRING, offset, item.sourceSnapshot().size(), attempt,
                        "file block transferred", null,
                        Map.of("activityBytes", activityBaseBytes,
                                "resumedBytes", resumedBytes)));
                throttle(runtime, offset - resumedBytes, attemptStartedNanos);
            }
            if (streamingDigest != null) {
                sourceChecksum = streamingDigest.digestHex();
            }

            transition(plan, item, state, TransferItemStatus.VERIFYING, offset, attempt, events,
                    "verifying source snapshot and target checksum", null);
            ensureSourceSnapshot(item.sourceSnapshot(), source.stat(item.sourcePath()));
            TransferFileEntry temporary = target.stat(item.temporaryPath());
            if (temporary.size() != item.sourceSnapshot().size()) {
                throw new IOException("temporary target size mismatch: expected "
                        + item.sourceSnapshot().size() + " but was " + temporary.size());
            }
            String targetChecksum = target.checksum(item.temporaryPath(), policy.checksumAlgorithm());
            if (!sourceChecksum.equals(targetChecksum)) {
                target.abort(resume.session());
                checkpoints.delete(plan.runId(), item.itemId());
                throw new RestartFromZeroException(
                        "SHA-256 verification failed; retained checkpoint was discarded");
            }

            transition(plan, item, state, TransferItemStatus.COMMITTING, offset, attempt, events,
                    "committing verified temporary target", null);
            commit(item, policy, target, resume.session(), targetExisted, originalTargetChecksum);
            checkpoints.delete(plan.runId(), item.itemId());

            if (policy.sourceSuccessAction() == SourceSuccessAction.KEEP) {
                transition(plan, item, state, TransferItemStatus.SUCCESS, offset, attempt, events,
                        "file transfer completed", null);
                return successResult(item, TransferItemStatus.SUCCESS, offset, resumedBytes, attempt,
                        sourceChecksum, targetChecksum, startedAt);
            }

            transition(plan, item, state, TransferItemStatus.POST_ACTION, offset, attempt, events,
                    "running source success action", null);
            try {
                postProcessSource(item, policy, source);
                transition(plan, item, state, TransferItemStatus.SUCCESS, offset, attempt, events,
                        "file transfer and source action completed", null);
                return successResult(item, TransferItemStatus.SUCCESS, offset, resumedBytes, attempt,
                        sourceChecksum, targetChecksum, startedAt);
            } catch (Exception e) {
                transition(plan, item, state, TransferItemStatus.POST_ACTION_FAILED,
                        offset, attempt, events, safeMessage(e), "POST_ACTION_FAILED");
                return new TransferItemResult(item.itemId(), TransferItemStatus.POST_ACTION_FAILED,
                        item.sourceSnapshot().size(), offset, resumedBytes, attempt,
                        sourceChecksum, targetChecksum, "POST_ACTION_FAILED", safeMessage(e),
                        startedAt, Instant.now());
            }
            }
        }
    }

    private MessageDigest newDigest(String algorithm) {
        try {
            return MessageDigest.getInstance(algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalArgumentException("unsupported checksum algorithm: " + algorithm, e);
        }
    }

    private void updateDigest(InputStream input, long length, ResumableSha256 digest, String path)
            throws IOException {
        byte[] buffer = new byte[64 * 1024];
        long remaining = length;
        while (remaining > 0) {
            int read = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
            if (read < 0) {
                throw new IOException("unexpected EOF while checksumming " + path);
            }
            if (read == 0) {
                continue;
            }
            digest.update(buffer, 0, read);
            remaining -= read;
        }
    }

    private void publishChecksumRebuildProgress(TransferPlan plan, TransferPlanItem item,
                                                TransferItemStateMachine state, TransferControl control,
                                                TransferEventListener events, int attempt,
                                                long confirmedOffset, long resumedBytes,
                                                long activityBytes) throws IOException {
        try {
            pauseOrCancel(plan, item, state, control, events, attempt, confirmedOffset,
                    "transfer paused while validating retained checkpoint checksum",
                    "checkpoint checksum validation resumed");
        } catch (TransferCanceledException e) {
            throw new IOException("transfer canceled while validating retained checkpoint checksum", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.io.InterruptedIOException(
                    "transfer interrupted while validating retained checkpoint checksum");
        }
        events.onEvent(event(TransferEventType.ITEM_PROGRESS, plan.runId(), item.itemId(),
                TransferItemStatus.TRANSFERRING, confirmedOffset,
                item.sourceSnapshot().size(), attempt,
                "validating retained checkpoint checksum", null,
                Map.of("live", true,
                        "confirmedBytes", confirmedOffset,
                        "observedBytes", confirmedOffset,
                        "activityBytes", activityBytes,
                        "resumePhase", "REBUILDING_CHECKSUM",
                        "resumedBytes", resumedBytes)));
    }

    private ResumeSession prepareSession(TransferPlan plan, TransferPlanItem item,
                                         TransferFileSystem source, TransferFileSystem target,
                                         TransferCheckpointStore checkpoints,
                                         CheckpointRecoveryMode recoveryMode,
                                         boolean checksumAlreadyKnown) throws IOException {
        Optional<TransferCheckpoint> existingCheckpoint = checkpoints.load(plan.runId(), item.itemId());
        TransferWriteSession session = target.prepareWrite(item.temporaryPath(), item.itemId());
        if (existingCheckpoint.isPresent()) {
            TransferCheckpoint checkpoint = existingCheckpoint.get();
            if (!checkpoint.matches(plan, item)) {
                return restartSession(plan, item, target, checkpoints, session,
                        "checkpoint identity or source snapshot changed; transfer restarted");
            }
            if (session.confirmedOffset() > item.sourceSnapshot().size()) {
                return restartSession(plan, item, target, checkpoints, session,
                        "temporary target exceeded source size; transfer restarted");
            }
            if (session.confirmedOffset() < checkpoint.confirmedOffset()) {
                return restartSession(plan, item, target, checkpoints, session,
                        "temporary target was shorter than checkpoint; transfer restarted");
            }
            if (session.confirmedOffset() > checkpoint.confirmedOffset()) {
                Optional<TransferWriteSession> recovered = target.recoverWriteSession(
                        session, checkpoint.confirmedOffset());
                if (recovered.isEmpty()
                        || recovered.get().confirmedOffset() != checkpoint.confirmedOffset()) {
                    return restartSession(plan, item, target, checkpoints, session,
                            "temporary target could not be aligned to checkpoint; transfer restarted");
                }
                session = recovered.get();
            }
            if (session.confirmedOffset() == 0L || checksumAlreadyKnown) {
                return new ResumeSession(session, null, false, null, null);
            }
            if (checkpoint.hasResumableChecksumState()) {
                byte[] headFingerprint = checkpoint.hasBoundaryFingerprint()
                        ? validatedHeadFingerprint(source, item.sourcePath(), checkpoint)
                        : null;
                if (headFingerprint == null) {
                    return restartSession(plan, item, target, checkpoints, session,
                            "checkpoint boundary validation failed; transfer restarted");
                }
                try {
                    return new ResumeSession(session,
                            ResumableSha256.restore(checkpoint.checksumStateBase64()), false, null,
                            headFingerprint);
                } catch (IOException invalidState) {
                    return restartSession(plan, item, target, checkpoints, session,
                            "checkpoint checksum state was invalid; transfer restarted");
                }
            }
            if (recoveryMode == CheckpointRecoveryMode.STRICT) {
                int headLength = (int) Math.min(64 * 1024L, checkpoint.confirmedOffset());
                return new ResumeSession(session, ResumableSha256.create(), true, null,
                        readBytes(source, item.sourcePath(), 0L, headLength));
            }
            return restartSession(plan, item, target, checkpoints, session,
                    "legacy checkpoint has no resumable checksum state; transfer restarted");
        } else if (session.confirmedOffset() > 0) {
            return restartSession(plan, item, target, checkpoints, session,
                    "stale temporary target had no checkpoint; transfer restarted");
        }
        return new ResumeSession(session, null, false, null, null);
    }

    private ResumeSession restartSession(TransferPlan plan, TransferPlanItem item,
                                         TransferFileSystem target,
                                         TransferCheckpointStore checkpoints,
                                         TransferWriteSession session,
                                         String reason) throws IOException {
        target.abort(session);
        checkpoints.delete(plan.runId(), item.itemId());
        TransferWriteSession restarted = target.prepareWrite(item.temporaryPath(), item.itemId());
        if (restarted.confirmedOffset() != 0L) {
            throw new IOException("stale temporary target could not be reset");
        }
        return new ResumeSession(restarted, null, false, reason, null);
    }

    private byte[] validatedHeadFingerprint(TransferFileSystem source, String sourcePath,
                                            TransferCheckpoint checkpoint) throws IOException {
        byte[] head = readBytes(source, sourcePath, checkpoint.headOffset(), checkpoint.headLength());
        if (!checkpoint.headSha256().equalsIgnoreCase(sha256(head))) {
            return null;
        }
        boolean sameRange = checkpoint.headOffset() == checkpoint.boundaryOffset()
                && checkpoint.headLength() == checkpoint.boundaryLength();
        byte[] boundary = sameRange ? head : readBytes(source, sourcePath,
                checkpoint.boundaryOffset(), checkpoint.boundaryLength());
        return checkpoint.boundarySha256().equalsIgnoreCase(sha256(boundary)) ? head : null;
    }

    private byte[] readBytes(TransferFileSystem source, String sourcePath,
                             long offset, int length) throws IOException {
        byte[] value = new byte[length];
        int position = 0;
        try (InputStream input = source.openRead(sourcePath, offset, length)) {
            while (position < length) {
                int read = input.read(value, position, length - position);
                if (read < 0) {
                    throw new IOException("unexpected EOF while validating checkpoint fingerprint");
                }
                if (read > 0) {
                    position += read;
                }
            }
        }
        return value;
    }

    private String sha256(byte[] value) {
        MessageDigest digest = newDigest("SHA-256");
        digest.update(value);
        return java.util.HexFormat.of().formatHex(digest.digest());
    }

    private void commit(TransferPlanItem item, TransferPolicy policy, TransferFileSystem target,
                        TransferWriteSession session, boolean targetExisted,
                        String originalTargetChecksum) throws IOException {
        boolean existsNow = target.transferExists(item.targetPath());
        if (targetExisted && !existsNow) {
            throw new IOException("target changed during transfer: original target disappeared");
        }
        if (targetExisted) {
            String currentChecksum = target.checksum(item.targetPath(), policy.checksumAlgorithm());
            if (!currentChecksum.equals(originalTargetChecksum)) {
                throw new IOException("target changed during transfer");
            }
        }
        if (policy.conflictPolicy() == ConflictPolicy.BACKUP_THEN_OVERWRITE && existsNow) {
            String backupPath = item.targetPath() + ".backup."
                    + DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS")
                    .withZone(java.time.ZoneOffset.UTC).format(Instant.now())
                    + "." + item.itemId();
            target.move(item.targetPath(), backupPath, false);
            try {
                target.commit(session, item.targetPath(), false);
            } catch (IOException commitError) {
                try {
                    if (!target.transferExists(item.targetPath())) {
                        target.move(backupPath, item.targetPath(), false);
                    }
                } catch (IOException restoreError) {
                    commitError.addSuppressed(restoreError);
                }
                throw commitError;
            }
            return;
        }
        boolean replace = policy.conflictPolicy() == ConflictPolicy.OVERWRITE;
        target.commit(session, item.targetPath(), replace);
    }

    private void postProcessSource(TransferPlanItem item, TransferPolicy policy,
                                   TransferFileSystem source) throws IOException {
        if (policy.sourceSuccessAction() == SourceSuccessAction.DELETE) {
            source.delete(item.sourcePath());
            return;
        }
        if (policy.sourceSuccessAction() == SourceSuccessAction.BACKUP) {
            String backupPath = TransferPaths.join(policy.sourceBackupRootPath(), item.relativePath());
            if (TransferPaths.normalize(backupPath).equalsIgnoreCase(TransferPaths.normalize(item.sourcePath()))) {
                throw new IOException("source backup path must differ from source path");
            }
            source.move(item.sourcePath(), backupPath, false);
        }
    }

    private long pauseOrCancel(TransferPlan plan, TransferPlanItem item,
                               TransferItemStateMachine state, TransferControl control,
                               TransferEventListener events, int attempt, long offset)
            throws InterruptedException, TransferCanceledException {
        return pauseOrCancel(plan, item, state, control, events, attempt, offset,
                "transfer paused at block boundary", "transfer resumed");
    }

    private long pauseOrCancel(TransferPlan plan, TransferPlanItem item,
                               TransferItemStateMachine state, TransferControl control,
                               TransferEventListener events, int attempt, long offset,
                               String pauseMessage, String resumeMessage)
            throws InterruptedException, TransferCanceledException {
        if (control.isCanceled()) {
            throw new TransferCanceledException();
        }
        if (control.isPauseRequested()) {
            transition(plan, item, state, TransferItemStatus.PAUSED, offset, attempt, events,
                    pauseMessage, null);
            control.awaitIfPaused();
            transition(plan, item, state, TransferItemStatus.TRANSFERRING, offset, attempt, events,
                    resumeMessage, null);
        }
        return offset;
    }

    private void enterTransferring(TransferPlan plan, TransferPlanItem item,
                                   TransferItemStateMachine state, TransferControl control,
                                   TransferEventListener events, int attempt, long offset,
                                   boolean rebuildingChecksum, String restartReason)
            throws InterruptedException, TransferCanceledException {
        Map<String, Object> details = offset > 0
                ? Map.of("resumedBytes", offset, "resumePhase",
                        rebuildingChecksum ? "REBUILDING_CHECKSUM" : "RESUMING_TRANSFER",
                        "activityBytes", offset)
                : restartReason == null ? Map.of() : Map.of(
                        "resumePhase", "RESTARTED_FROM_ZERO",
                        "restartReason", restartReason);
        if (control.isPauseRequested()) {
            transition(plan, item, state, TransferItemStatus.PAUSED, offset, attempt, events,
                    offset > 0 ? "transfer remains paused at retained checkpoint"
                            : "transfer paused before start",
                    null, details);
            control.awaitIfPaused();
        }
        if (control.isCanceled()) {
            throw new TransferCanceledException();
        }
        transition(plan, item, state, TransferItemStatus.TRANSFERRING, offset, attempt, events,
                restartReason != null ? restartReason
                        : offset <= 0 ? "file transfer started"
                        : rebuildingChecksum
                                ? "checkpoint restored; rebuilding checksum before transfer resumes"
                                : "checkpoint restored; transfer resumes from confirmed offset",
                null, details);
    }

    private void ensureSourceSnapshot(SourceSnapshot snapshot, TransferFileEntry current) throws IOException {
        if (!snapshot.matches(current)) {
            throw new IOException("source file changed after the transfer plan was created");
        }
    }

    private void throttle(TransferRuntimeOptions runtime, long writtenBytes, long startedNanos)
            throws InterruptedException {
        if (runtime.maxBytesPerSecond() <= 0 || writtenBytes <= 0) {
            return;
        }
        long perFileLimit = Math.max(1L, runtime.maxBytesPerSecond() / runtime.concurrency());
        long expectedNanos = (long) ((double) writtenBytes * 1_000_000_000D / perFileLimit);
        long remainingNanos = expectedNanos - (System.nanoTime() - startedNanos);
        if (remainingNanos > 0) {
            TimeUnit.NANOSECONDS.sleep(remainingNanos);
        }
    }

    private void sleepWithControl(long delayMillis, TransferControl control)
            throws InterruptedException, TransferCanceledException {
        long remaining = delayMillis;
        while (remaining > 0) {
            if (control.isCanceled()) {
                throw new TransferCanceledException();
            }
            long sleep = Math.min(remaining, 250L);
            Thread.sleep(sleep);
            remaining -= sleep;
        }
    }

    private void transition(TransferPlan plan, TransferPlanItem item, TransferItemStateMachine state,
                            TransferItemStatus next, long transferred, int attempt,
                            TransferEventListener events, String message, String errorCode) {
        transition(plan, item, state, next, transferred, attempt, events, message, errorCode, Map.of());
    }

    private void transition(TransferPlan plan, TransferPlanItem item, TransferItemStateMachine state,
                            TransferItemStatus next, long transferred, int attempt,
                            TransferEventListener events, String message, String errorCode,
                            Map<String, Object> details) {
        state.transitionTo(next);
        events.onEvent(event(TransferEventType.ITEM_STATUS_CHANGED, plan.runId(), item.itemId(), next,
                transferred, item.sourceSnapshot().size(), attempt, message, errorCode, details));
    }

    private TransferResult result(TransferPlan plan, List<TransferItemResult> items,
                                  Instant startedAt, Instant finishedAt) {
        long success = items.stream().filter(item -> item.status() == TransferItemStatus.SUCCESS).count();
        long skipped = items.stream().filter(item -> item.status() == TransferItemStatus.SKIPPED).count();
        long failed = items.size() - success - skipped;
        TransferRunStatus status;
        if (items.stream().anyMatch(item -> item.status() == TransferItemStatus.CANCELED)) {
            status = TransferRunStatus.CANCELED;
        } else if (failed == 0) {
            status = TransferRunStatus.SUCCESS;
        } else if (success + skipped > 0
                || items.stream().anyMatch(item -> item.status() == TransferItemStatus.POST_ACTION_FAILED)) {
            status = TransferRunStatus.PARTIAL_SUCCESS;
        } else {
            status = TransferRunStatus.FAILED;
        }
        return new TransferResult(plan.runId(), status, items, items.size(), success, skipped, failed,
                totalBytes(plan), items.stream().mapToLong(TransferItemResult::transferredBytes).sum(),
                items.stream().mapToLong(TransferItemResult::resumedBytes).sum(), startedAt, finishedAt);
    }

    private TransferItemResult successResult(TransferPlanItem item, TransferItemStatus status,
                                             long transferred, long resumed, int attempts,
                                             String sourceChecksum, String targetChecksum,
                                             Instant startedAt) {
        return new TransferItemResult(item.itemId(), status, item.sourceSnapshot().size(), transferred,
                resumed, attempts, sourceChecksum, targetChecksum, null, null, startedAt, Instant.now());
    }

    private TransferItemResult failureResult(TransferPlanItem item, TransferItemStatus status,
                                             int attempts, String errorCode, String errorMessage,
                                             Instant startedAt) {
        return new TransferItemResult(item.itemId(), status, item.sourceSnapshot().size(), 0, 0,
                attempts, null, null, errorCode, errorMessage, startedAt, Instant.now());
    }

    private long totalBytes(TransferPlan plan) {
        return plan.items().stream().mapToLong(item -> item.sourceSnapshot().size()).sum();
    }

    private boolean retryable(Exception error) {
        return !(error instanceof FileAlreadyExistsException)
                && !(error instanceof IllegalArgumentException);
    }

    private boolean isCanceled(Throwable error) {
        Throwable current = error;
        while (current != null) {
            if (current instanceof TransferCanceledException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private String errorCode(Exception error) {
        String message = safeMessage(error).toLowerCase();
        if (message.contains("source file changed")) {
            return "SOURCE_CHANGED";
        }
        if (message.contains("checkpoint") || message.contains("temporary target offset")) {
            return "CHECKPOINT_INVALID";
        }
        if (message.contains("unexpected eof")) {
            return "UNEXPECTED_EOF";
        }
        if (message.contains("sha-256") || message.contains("checksum")) {
            return "CHECKSUM_MISMATCH";
        }
        if (message.contains("target changed")) {
            return "TARGET_CHANGED";
        }
        return error instanceof IOException ? "IO_ERROR" : "INTERNAL_ERROR";
    }

    private String safeMessage(Throwable error) {
        return error.getMessage() == null || error.getMessage().isBlank()
                ? error.getClass().getSimpleName()
                : error.getMessage();
    }

    private TransferEvent event(TransferEventType type, String runId, String itemId,
                                TransferItemStatus status, long transferred, long total,
                                int attempt, String message, String errorCode,
                                Map<String, Object> details) {
        return new TransferEvent(type, Instant.now(), runId, itemId, status, transferred,
                total, attempt, message, errorCode, details);
    }

    private record ResumeSession(TransferWriteSession session,
                                 ResumableSha256 checksum,
                                 boolean rebuildingChecksum,
                                 String restartReason,
                                 byte[] headFingerprint) {
    }

    private static final class RestartFromZeroException extends IOException {
        private RestartFromZeroException(String message) {
            super(message);
        }
    }

    @FunctionalInterface
    private interface LiveProgressObserver {
        void observed(long observedBytes) throws IOException;
    }

    private static final class LiveProgressInputStream extends InputStream {
        private final InputStream delegate;
        private final long confirmedOffset;
        private final long intervalNanos;
        private final LiveProgressObserver observer;
        private long bytesRead;
        private long lastObservedAt;

        private LiveProgressInputStream(InputStream delegate, long confirmedOffset,
                                        long intervalMillis, LiveProgressObserver observer) {
            this.delegate = delegate;
            this.confirmedOffset = confirmedOffset;
            this.intervalNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(1L, intervalMillis));
            this.observer = observer;
            this.lastObservedAt = System.nanoTime();
        }

        @Override
        public int read() throws IOException {
            int value = delegate.read();
            if (value >= 0) {
                recordRead(1L);
            }
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int read = delegate.read(buffer, offset, length);
            if (read > 0) {
                recordRead(read);
            }
            return read;
        }

        @Override
        public long skip(long count) throws IOException {
            long skipped = delegate.skip(count);
            if (skipped > 0L) {
                recordRead(skipped);
            }
            return skipped;
        }

        @Override
        public int available() throws IOException {
            return delegate.available();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        private void recordRead(long count) throws IOException {
            bytesRead += count;
            long now = System.nanoTime();
            if (now - lastObservedAt < intervalNanos) {
                return;
            }
            lastObservedAt = now;
            observer.observed(confirmedOffset + bytesRead);
        }
    }

    private static final class CheckpointDigestInputStream extends InputStream {
        private static final int FINGERPRINT_BYTES = 64 * 1024;

        private final InputStream delegate;
        private final ResumableSha256 digest;
        private final byte[] tail = new byte[FINGERPRINT_BYTES];
        private final byte[] head = new byte[FINGERPRINT_BYTES];
        private int headLength;
        private int tailLength;
        private int tailPosition;

        private CheckpointDigestInputStream(InputStream delegate, ResumableSha256 digest) {
            this.delegate = delegate;
            this.digest = digest;
        }

        @Override
        public int read() throws IOException {
            int value = delegate.read();
            if (value >= 0) {
                byte[] single = {(byte) value};
                record(single, 0, 1);
            }
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            int read = delegate.read(buffer, offset, length);
            if (read > 0) {
                record(buffer, offset, read);
            }
            return read;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        private void record(byte[] buffer, int offset, int length) {
            digest.update(buffer, offset, length);
            int headCopyLength = Math.min(length, head.length - headLength);
            if (headCopyLength > 0) {
                System.arraycopy(buffer, offset, head, headLength, headCopyLength);
                headLength += headCopyLength;
            }
            for (int index = 0; index < length; index++) {
                tail[tailPosition] = buffer[offset + index];
                tailPosition = (tailPosition + 1) % tail.length;
                tailLength = Math.min(tail.length, tailLength + 1);
            }
        }

        private int tailLength() {
            return tailLength;
        }

        private byte[] headBytes() {
            return java.util.Arrays.copyOf(head, headLength);
        }

        private String tailSha256() {
            MessageDigest boundaryDigest;
            try {
                boundaryDigest = MessageDigest.getInstance("SHA-256");
            } catch (NoSuchAlgorithmException exception) {
                throw new IllegalStateException("SHA-256 is not available", exception);
            }
            int start = tailLength == tail.length ? tailPosition : 0;
            int first = Math.min(tailLength, tail.length - start);
            boundaryDigest.update(tail, start, first);
            if (first < tailLength) {
                boundaryDigest.update(tail, 0, tailLength - first);
            }
            return java.util.HexFormat.of().formatHex(boundaryDigest.digest());
        }
    }
}
