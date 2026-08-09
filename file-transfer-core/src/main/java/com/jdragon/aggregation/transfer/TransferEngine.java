package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.datasource.file.transfer.TransferWriteSession;
import com.jdragon.aggregation.transfer.model.ConflictPolicy;
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
                String errorCode = errorCode(e);
                transition(plan, item, state, TransferItemStatus.FAILED, 0, attempts, events,
                        safeMessage(e), errorCode);
                if (attempts > runtime.maxRetries() || !retryable(e)) {
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

            String sourceChecksum = source.checksum(item.sourcePath(), policy.checksumAlgorithm());
            try (TransferFileSystem target = targetFactory.open()) {
            boolean targetExisted = target.transferExists(item.targetPath());
            String originalTargetChecksum = null;
            if (targetExisted) {
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

            enterTransferring(plan, item, state, control, events, attempt, 0);
            ResumeSession resume = prepareSession(plan, item, target, checkpoints);
            long offset = resume.session().confirmedOffset();
            long resumedBytes = offset;
            long attemptStartedNanos = System.nanoTime();
            while (offset < item.sourceSnapshot().size()) {
                offset = pauseOrCancel(plan, item, state, control, events, attempt, offset);
                long length = Math.min(runtime.chunkSizeBytes(), item.sourceSnapshot().size() - offset);
                long written;
                try (InputStream input = source.openRead(item.sourcePath(), offset, length)) {
                    written = target.append(resume.session(), offset, input, length);
                }
                if (written != length) {
                    throw new IOException("unexpected EOF: expected " + length + " bytes but wrote " + written);
                }
                offset += written;
                TransferCheckpoint checkpoint = new TransferCheckpoint(plan.runId(), item.itemId(),
                        item.sourceIdentity(), item.targetIdentity(), item.sourceSnapshot(),
                        item.temporaryPath(), offset, Instant.now());
                checkpoints.save(checkpoint);
                events.onEvent(event(TransferEventType.CHECKPOINT_SAVED, plan.runId(), item.itemId(),
                        TransferItemStatus.TRANSFERRING, offset, item.sourceSnapshot().size(), attempt,
                        "transfer checkpoint saved", null, Map.of()));
                events.onEvent(event(TransferEventType.ITEM_PROGRESS, plan.runId(), item.itemId(),
                        TransferItemStatus.TRANSFERRING, offset, item.sourceSnapshot().size(), attempt,
                        "file block transferred", null, Map.of("resumedBytes", resumedBytes)));
                throttle(runtime, offset - resumedBytes, attemptStartedNanos);
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
                throw new IOException("SHA-256 verification failed");
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

    private ResumeSession prepareSession(TransferPlan plan, TransferPlanItem item,
                                         TransferFileSystem target,
                                         TransferCheckpointStore checkpoints) throws IOException {
        Optional<TransferCheckpoint> existingCheckpoint = checkpoints.load(plan.runId(), item.itemId());
        TransferWriteSession session = target.prepareWrite(item.temporaryPath(), item.itemId());
        if (existingCheckpoint.isPresent()) {
            TransferCheckpoint checkpoint = existingCheckpoint.get();
            if (!checkpoint.matches(plan, item)) {
                throw new IOException("checkpoint identity or source snapshot does not match the transfer item");
            }
            if (session.confirmedOffset() != checkpoint.confirmedOffset()) {
                throw new IOException("temporary target offset does not match checkpoint: expected "
                        + checkpoint.confirmedOffset() + " but was " + session.confirmedOffset());
            }
        } else if (session.confirmedOffset() > 0) {
            target.abort(session);
            session = target.prepareWrite(item.temporaryPath(), item.itemId());
            if (session.confirmedOffset() != 0) {
                throw new IOException("stale temporary target could not be reset");
            }
        }
        if (session.confirmedOffset() > item.sourceSnapshot().size()) {
            throw new IOException("temporary target is larger than the source snapshot");
        }
        return new ResumeSession(session);
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
        if (control.isCanceled()) {
            throw new TransferCanceledException();
        }
        if (control.isPauseRequested()) {
            transition(plan, item, state, TransferItemStatus.PAUSED, offset, attempt, events,
                    "transfer paused at block boundary", null);
            control.awaitIfPaused();
            transition(plan, item, state, TransferItemStatus.TRANSFERRING, offset, attempt, events,
                    "transfer resumed", null);
        }
        return offset;
    }

    private void enterTransferring(TransferPlan plan, TransferPlanItem item,
                                   TransferItemStateMachine state, TransferControl control,
                                   TransferEventListener events, int attempt, long offset)
            throws InterruptedException, TransferCanceledException {
        if (control.isPauseRequested()) {
            transition(plan, item, state, TransferItemStatus.PAUSED, offset, attempt, events,
                    "transfer paused before start", null);
            control.awaitIfPaused();
        }
        if (control.isCanceled()) {
            throw new TransferCanceledException();
        }
        transition(plan, item, state, TransferItemStatus.TRANSFERRING, offset, attempt, events,
                "file transfer started", null);
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
        state.transitionTo(next);
        events.onEvent(event(TransferEventType.ITEM_STATUS_CHANGED, plan.runId(), item.itemId(), next,
                transferred, item.sourceSnapshot().size(), attempt, message, errorCode, Map.of()));
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

    private record ResumeSession(TransferWriteSession session) {
    }
}
