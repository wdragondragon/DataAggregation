package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.transfer.model.SourceSnapshot;
import com.jdragon.aggregation.transfer.model.TransferPlan;
import com.jdragon.aggregation.transfer.model.TransferPlanItem;
import com.jdragon.aggregation.transfer.model.TransferPolicy;
import com.jdragon.aggregation.transfer.model.VerificationMode;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.function.LongConsumer;

public final class TransferVerifier {

    public static final String PARTIAL_DIGEST_PREFIX = "PARTIAL-SHA256-V1:";
    private static final byte[] PARTIAL_DIGEST_DOMAIN =
            "FILE-TRANSFER-PARTIAL-SHA256-V1".getBytes(StandardCharsets.UTF_8);
    private static final int BUFFER_SIZE = 64 * 1024;

    public TransferVerificationPlan plan(TransferPlan transferPlan,
                                         TransferPlanItem item,
                                         TransferPolicy policy) {
        return plan(transferPlan, item, policy, item.sourceSnapshot().size(), "CONTENT");
    }

    public TransferVerificationPlan planForExistingTarget(TransferPlan transferPlan,
                                                          TransferPlanItem item,
                                                          TransferPolicy policy,
                                                          long targetSize) {
        // Equal-size source and target files must use the same sample offsets so
        // their partial fingerprints are comparable. A different target size is
        // already part of the seed and still produces its own concurrency plan.
        VerificationMode effectiveMode = policy.effectiveVerificationMode(item.sourceSnapshot().size());
        if (effectiveMode == VerificationMode.PARTIAL
                && configuredSampleBytes(policy) >= targetSize) {
            effectiveMode = VerificationMode.STRONG;
        }
        return plan(transferPlan, item, policy, targetSize, "CONTENT",
                policy.verificationMode(), effectiveMode);
    }

    public String checksum(TransferFileSystem fileSystem,
                           String path,
                           TransferVerificationPlan plan,
                           String algorithm,
                           LongConsumer progress) throws IOException {
        if (plan.effectiveMode() == VerificationMode.NONE) {
            return null;
        }
        if (plan.effectiveMode() == VerificationMode.STRONG) {
            return fileSystem.checksum(path, algorithm, progress);
        }
        ensureRangeRead(fileSystem, "data source");
        MessageDigest digest = sha256();
        digest.update(PARTIAL_DIGEST_DOMAIN);
        updateLong(digest, plan.fileSize());
        updateInt(digest, plan.frames().size());
        long processed = 0L;
        byte[] buffer = new byte[BUFFER_SIZE];
        LongConsumer observer = progress == null ? ignored -> { } : progress;
        for (TransferVerificationFrame frame : plan.frames()) {
            updateLong(digest, frame.offset());
            updateLong(digest, frame.length());
            long remaining = frame.length();
            try (InputStream input = fileSystem.openRead(path, frame.offset(), frame.length())) {
                while (remaining > 0L) {
                    int read = input.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                    if (read < 0) {
                        throw new IOException("unexpected EOF while sampling " + path);
                    }
                    if (read == 0) {
                        continue;
                    }
                    digest.update(buffer, 0, read);
                    remaining -= read;
                    processed += read;
                    observer.accept(processed);
                }
            }
        }
        return PARTIAL_DIGEST_PREFIX + HexFormat.of().formatHex(digest.digest());
    }

    public void ensureSupported(TransferFileSystem source,
                                TransferFileSystem target,
                                TransferVerificationPlan plan) throws PartialVerificationUnsupportedException {
        if (plan.effectiveMode() != VerificationMode.PARTIAL) {
            return;
        }
        ensureRangeRead(source, "source");
        ensureRangeRead(target, "target");
    }

    private TransferVerificationPlan plan(TransferPlan transferPlan,
                                          TransferPlanItem item,
                                          TransferPolicy policy,
                                          long fileSize,
                                          String scope) {
        return plan(transferPlan, item, policy, fileSize, scope,
                policy.verificationMode(), policy.effectiveVerificationMode(fileSize));
    }

    private TransferVerificationPlan plan(TransferPlan transferPlan,
                                          TransferPlanItem item,
                                          TransferPolicy policy,
                                          long fileSize,
                                          String scope,
                                          VerificationMode configured,
                                          VerificationMode effective) {
        if (effective == VerificationMode.NONE) {
            return new TransferVerificationPlan(configured, effective, fileSize, List.of(), 0L);
        }
        if (effective == VerificationMode.STRONG) {
            return new TransferVerificationPlan(configured, effective, fileSize, List.of(), fileSize);
        }

        int frameCount = policy.verificationFrameCount();
        long frameSize = policy.verificationFrameSizeBytes();
        byte[] seed = seed(transferPlan, item, policy, fileSize, scope);
        List<TransferVerificationFrame> frames = new ArrayList<>(frameCount);
        for (int index = 0; index < frameCount; index++) {
            long segmentStart = multiplyDivide(index, fileSize, frameCount);
            long segmentEnd = multiplyDivide(index + 1L, fileSize, frameCount);
            long availableStarts = segmentEnd - segmentStart - frameSize + 1L;
            if (availableStarts <= 0L) {
                throw new IllegalStateException("partial verification frame does not fit its segment");
            }
            long offset = segmentStart + deterministicOffset(seed, index, availableStarts);
            frames.add(new TransferVerificationFrame(offset, frameSize));
        }
        return new TransferVerificationPlan(configured, effective, fileSize, frames,
                Math.multiplyExact((long) frameCount, frameSize));
    }

    private long configuredSampleBytes(TransferPolicy policy) {
        return Math.multiplyExact((long) policy.verificationFrameCount(),
                policy.verificationFrameSizeBytes());
    }

    private byte[] seed(TransferPlan plan, TransferPlanItem item, TransferPolicy policy,
                        long fileSize, String scope) {
        MessageDigest digest = sha256();
        updateString(digest, "FILE-TRANSFER-SAMPLE-PLAN-V1");
        updateString(digest, scope);
        updateString(digest, plan.runId());
        updateString(digest, item.itemId());
        updateString(digest, item.sourceIdentity());
        SourceSnapshot snapshot = item.sourceSnapshot();
        updateString(digest, snapshot.path());
        updateLong(digest, snapshot.size());
        updateLong(digest, snapshot.modifiedTimeMillis());
        updateString(digest, snapshot.etag());
        updateLong(digest, fileSize);
        updateInt(digest, policy.verificationFrameCount());
        updateLong(digest, policy.verificationFrameSizeBytes());
        return digest.digest();
    }

    private long deterministicOffset(byte[] seed, int index, long bound) {
        MessageDigest digest = sha256();
        digest.update(seed);
        updateInt(digest, index);
        long value = ByteBuffer.wrap(digest.digest()).getLong() & Long.MAX_VALUE;
        return value % bound;
    }

    private long multiplyDivide(long value, long multiplier, long divisor) {
        long quotient = multiplier / divisor;
        long remainder = multiplier % divisor;
        return Math.addExact(Math.multiplyExact(value, quotient),
                Math.multiplyExact(value, remainder) / divisor);
    }

    private void ensureRangeRead(TransferFileSystem fileSystem, String role)
            throws PartialVerificationUnsupportedException {
        if (!fileSystem.capabilities().rangeRead()) {
            throw new PartialVerificationUnsupportedException(
                    "FILE_TRANSFER_PARTIAL_VERIFY_UNSUPPORTED: " + role + " does not support range reads");
        }
    }

    private MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private void updateString(MessageDigest digest, String value) {
        byte[] bytes = value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
        updateInt(digest, bytes.length);
        digest.update(bytes);
    }

    private void updateInt(MessageDigest digest, int value) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
    }

    private void updateLong(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }
}
