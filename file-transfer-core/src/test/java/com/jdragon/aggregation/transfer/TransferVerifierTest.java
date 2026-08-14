package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.local.LocalSourcePlugin;
import com.jdragon.aggregation.datasource.file.transfer.StorageCapabilities;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.transfer.model.ConflictPolicy;
import com.jdragon.aggregation.transfer.model.SourceSnapshot;
import com.jdragon.aggregation.transfer.model.SourceSuccessAction;
import com.jdragon.aggregation.transfer.model.TransferPlan;
import com.jdragon.aggregation.transfer.model.TransferPlanItem;
import com.jdragon.aggregation.transfer.model.TransferPolicy;
import com.jdragon.aggregation.transfer.model.VerificationMode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TransferVerifierTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final TransferVerifier verifier = new TransferVerifier();

    @Test
    public void historicalPolicyDefaultsToStrongVerification() {
        TransferPolicy policy = new TransferPolicy(ConflictPolicy.FAIL, "SHA-256",
                null, null, null, SourceSuccessAction.KEEP, null);

        assertEquals(VerificationMode.STRONG, policy.verificationMode());
        assertEquals(Integer.valueOf(16), policy.verificationFrameCount());
        assertEquals(Long.valueOf(1024L * 1024L), policy.verificationFrameSizeBytes());
    }

    @Test
    public void validatesPartialConfigurationAndNoneSourceAction() {
        assertThrows(IllegalArgumentException.class, () -> partial(0, 64 * 1024L));
        assertThrows(IllegalArgumentException.class, () -> partial(65, 64 * 1024L));
        assertThrows(IllegalArgumentException.class, () -> partial(4, 96 * 1024L));
        assertThrows(IllegalArgumentException.class, () -> new TransferPolicy(
                ConflictPolicy.FAIL, "SHA-256", VerificationMode.NONE, 16, 1024L * 1024L,
                SourceSuccessAction.DELETE, null));
    }

    @Test
    public void createsStableDistributedNonOverlappingFrames() {
        TransferPolicy policy = partial(8, 64 * 1024L);
        TransferPlanItem item = item(4L * 1024L * 1024L);
        TransferVerificationPlan first = verifier.plan(plan("run-a", item), item, policy);
        TransferVerificationPlan retry = verifier.plan(plan("run-a", item), item, policy);
        TransferVerificationPlan anotherRun = verifier.plan(plan("run-b", item), item, policy);

        assertEquals(first.frames(), retry.frames());
        assertNotEquals(first.frames(), anotherRun.frames());
        assertEquals(VerificationMode.PARTIAL, first.effectiveMode());
        assertEquals(8L * 64L * 1024L, first.verificationBytes());
        for (int index = 1; index < first.frames().size(); index++) {
            TransferVerificationFrame previous = first.frames().get(index - 1);
            TransferVerificationFrame current = first.frames().get(index);
            assertTrue(previous.offset() + previous.length() <= current.offset());
        }
    }

    @Test
    public void promotesPartialVerificationWhenConfiguredSamplesCoverTheFile() {
        TransferPolicy policy = partial(16, 64 * 1024L);
        TransferPlanItem item = item(1024L * 1024L);

        TransferVerificationPlan plan = verifier.plan(plan("run-promoted", item), item, policy);

        assertEquals(VerificationMode.PARTIAL, plan.configuredMode());
        assertEquals(VerificationMode.STRONG, plan.effectiveMode());
        assertTrue(plan.frames().isEmpty());
        assertEquals(item.sourceSnapshot().size(), plan.verificationBytes());
    }

    @Test
    public void sampledFingerprintChangesWhenASampledFrameChanges() throws Exception {
        Path root = temporaryFolder.newFolder("partial-checksum").toPath();
        byte[] content = new byte[2 * 1024 * 1024];
        for (int index = 0; index < content.length; index++) {
            content[index] = (byte) (index * 31);
        }
        Path file = root.resolve("sample.bin");
        Files.write(file, content);
        TransferPolicy policy = partial(4, 64 * 1024L);
        TransferPlanItem item = item(content.length);
        TransferVerificationPlan plan = verifier.plan(plan("run-checksum", item), item, policy);

        String before;
        try (LocalSourcePlugin source = connected(root)) {
            before = verifier.checksum(source, file.toString(), plan, "SHA-256", ignored -> { });
        }
        TransferVerificationFrame frame = plan.frames().get(0);
        content[(int) frame.offset()] ^= 0x5a;
        Files.write(file, content);
        String after;
        try (LocalSourcePlugin source = connected(root)) {
            after = verifier.checksum(source, file.toString(), plan, "SHA-256", ignored -> { });
        }

        assertTrue(before.startsWith(TransferVerifier.PARTIAL_DIGEST_PREFIX));
        assertEquals(TransferVerifier.PARTIAL_DIGEST_PREFIX.length() + 64, before.length());
        assertNotEquals(before, after);
    }

    @Test
    public void nonePlanHasNoContentVerification() {
        TransferPolicy policy = new TransferPolicy(ConflictPolicy.FAIL, "SHA-256",
                VerificationMode.NONE, 16, 1024L * 1024L, SourceSuccessAction.KEEP, null);
        TransferPlanItem item = item(10L * 1024L * 1024L);

        TransferVerificationPlan plan = verifier.plan(plan("run-none", item), item, policy);

        assertEquals(VerificationMode.NONE, plan.effectiveMode());
        assertEquals(0L, plan.verificationBytes());
        assertFalse(plan.phase().isBlank());
    }

    @Test
    public void existingTargetKeepsTheFilesPromotedStrongMode() {
        TransferPolicy policy = partial(16, 1024L * 1024L);
        TransferPlanItem item = item(1024L * 1024L);

        TransferVerificationPlan targetPlan = verifier.planForExistingTarget(
                plan("run-promoted-target", item), item, policy, 64L * 1024L * 1024L);

        assertEquals(VerificationMode.PARTIAL, targetPlan.configuredMode());
        assertEquals(VerificationMode.STRONG, targetPlan.effectiveMode());
        assertEquals(64L * 1024L * 1024L, targetPlan.verificationBytes());
    }

    @Test
    public void partialVerificationRejectsFilesystemsWithoutRangeReads() {
        TransferPolicy policy = partial(4, 64L * 1024L);
        TransferPlanItem item = item(2L * 1024L * 1024L);
        TransferVerificationPlan verificationPlan = verifier.plan(plan("run-range", item), item, policy);

        PartialVerificationUnsupportedException sourceError = assertThrows(
                PartialVerificationUnsupportedException.class,
                () -> verifier.ensureSupported(fileSystem(false), fileSystem(true), verificationPlan));
        PartialVerificationUnsupportedException targetError = assertThrows(
                PartialVerificationUnsupportedException.class,
                () -> verifier.ensureSupported(fileSystem(true), fileSystem(false), verificationPlan));

        assertTrue(sourceError.getMessage().contains("FILE_TRANSFER_PARTIAL_VERIFY_UNSUPPORTED"));
        assertTrue(targetError.getMessage().contains("FILE_TRANSFER_PARTIAL_VERIFY_UNSUPPORTED"));
    }

    private TransferPolicy partial(int count, long size) {
        return new TransferPolicy(ConflictPolicy.FAIL, "SHA-256", VerificationMode.PARTIAL,
                count, size, SourceSuccessAction.KEEP, null);
    }

    private TransferPlan plan(String runId, TransferPlanItem item) {
        return new TransferPlan(runId, Instant.parse("2026-08-14T00:00:00Z"), List.of(item));
    }

    private TransferPlanItem item(long size) {
        SourceSnapshot snapshot = new SourceSnapshot("/sample.bin", size, 1234L, "etag-1");
        return new TransferPlanItem("item-1", "source-1", "target-1", "/sample.bin",
                "/sample.bin", "/.sample.bin.part", "sample.bin", snapshot);
    }

    private LocalSourcePlugin connected(Path root) {
        Configuration configuration = Configuration.newDefault();
        configuration.set("rootPath", root.toString());
        LocalSourcePlugin plugin = new LocalSourcePlugin();
        plugin.connect(configuration);
        return plugin;
    }

    private TransferFileSystem fileSystem(boolean rangeRead) {
        StorageCapabilities capabilities = new StorageCapabilities(rangeRead, true, true,
                false, false, false, Set.of("SHA-256"), true);
        return (TransferFileSystem) Proxy.newProxyInstance(
                TransferFileSystem.class.getClassLoader(),
                new Class<?>[]{TransferFileSystem.class},
                (proxy, method, args) -> {
                    if ("capabilities".equals(method.getName())) {
                        return capabilities;
                    }
                    if (method.getDeclaringClass() == Object.class) {
                        return method.invoke(this, args);
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
