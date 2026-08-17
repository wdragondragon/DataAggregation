package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.local.LocalSourcePlugin;
import com.jdragon.aggregation.transfer.model.ConflictPolicy;
import com.jdragon.aggregation.transfer.model.SourceSuccessAction;
import com.jdragon.aggregation.transfer.model.TransferEndpoint;
import com.jdragon.aggregation.transfer.model.TransferItemResult;
import com.jdragon.aggregation.transfer.model.TransferItemStatus;
import com.jdragon.aggregation.transfer.model.TransferMapping;
import com.jdragon.aggregation.transfer.model.TransferPolicy;
import com.jdragon.aggregation.transfer.model.TransferResult;
import com.jdragon.aggregation.transfer.model.TransferRuntimeOptions;
import com.jdragon.aggregation.transfer.model.TransferSelection;
import com.jdragon.aggregation.transfer.model.TransferSpec;
import com.jdragon.aggregation.transfer.model.VerificationMode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TransferEngineVerificationModesTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void partialModeStoresSampledFingerprints() throws Exception {
        Path sourceRoot = temporaryFolder.newFolder("partial-source").toPath();
        Path targetRoot = temporaryFolder.newFolder("partial-target").toPath();
        byte[] content = new byte[2 * 1024 * 1024];
        java.util.Arrays.fill(content, (byte) 7);
        Files.write(sourceRoot.resolve("item.bin"), content);
        TransferPolicy policy = new TransferPolicy(ConflictPolicy.FAIL, "SHA-256",
                VerificationMode.PARTIAL, 4, 64L * 1024L, SourceSuccessAction.KEEP, null);

        TransferResult result = execute(sourceRoot, targetRoot, policy, "run-partial");

        TransferItemResult item = result.items().get(0);
        assertEquals(TransferItemStatus.SUCCESS, item.status());
        assertTrue(item.sourceChecksum().startsWith(TransferVerifier.PARTIAL_DIGEST_PREFIX));
        assertEquals(item.sourceChecksum(), item.targetChecksum());
    }

    @Test
    public void partialModeSkipsAnExistingTargetWithTheSameSamples() throws Exception {
        Path sourceRoot = temporaryFolder.newFolder("partial-skip-source").toPath();
        Path targetRoot = temporaryFolder.newFolder("partial-skip-target").toPath();
        byte[] content = new byte[2 * 1024 * 1024];
        for (int index = 0; index < content.length; index++) {
            content[index] = (byte) (index * 17);
        }
        Files.write(sourceRoot.resolve("item.bin"), content);
        Files.write(targetRoot.resolve("item.bin"), content);
        TransferPolicy policy = new TransferPolicy(ConflictPolicy.FAIL, "SHA-256",
                VerificationMode.PARTIAL, 4, 64L * 1024L, SourceSuccessAction.KEEP, null);

        TransferResult result = execute(sourceRoot, targetRoot, policy, "run-partial-skip");

        TransferItemResult item = result.items().get(0);
        assertEquals(TransferItemStatus.SKIPPED, item.status());
        assertTrue(item.sourceChecksum().startsWith(TransferVerifier.PARTIAL_DIGEST_PREFIX));
        assertEquals(item.sourceChecksum(), item.targetChecksum());
        assertEquals(0L, item.transferredBytes());
    }

    @Test
    public void noneModeNeverSkipsAnExistingTargetByContent() throws Exception {
        Path sourceRoot = temporaryFolder.newFolder("none-source").toPath();
        Path targetRoot = temporaryFolder.newFolder("none-target").toPath();
        byte[] content = "same-content".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        Files.write(sourceRoot.resolve("item.bin"), content);
        Files.write(targetRoot.resolve("item.bin"), content);
        TransferPolicy policy = new TransferPolicy(ConflictPolicy.FAIL, "SHA-256",
                VerificationMode.NONE, 16, 1024L * 1024L, SourceSuccessAction.KEEP, null);

        TransferResult result = execute(sourceRoot, targetRoot, policy, "run-none");

        TransferItemResult item = result.items().get(0);
        assertEquals(TransferItemStatus.CONFLICT, item.status());
        assertNull(item.sourceChecksum());
        assertNull(item.targetChecksum());
    }

    private TransferResult execute(Path sourceRoot, Path targetRoot,
                                   TransferPolicy policy, String runId) throws Exception {
        TransferSpec spec = new TransferSpec(1,
                new TransferEndpoint("local", "source", Map.of("rootPath", sourceRoot.toString())),
                new TransferEndpoint("local", "target", Map.of("rootPath", targetRoot.toString())),
                new TransferSelection(sourceRoot.toString(), List.of("item.bin"), true,
                        List.of(), null, List.of(), null, null, null, null, 100),
                new TransferMapping(targetRoot.toString(), true, null), policy,
                new TransferRuntimeOptions(1, 0, List.of(0L), 64L * 1024L, 1000L, 0L),
                "UTC", Map.of());
        PreparedTransfer prepared;
        try (LocalSourcePlugin source = connected(sourceRoot)) {
            prepared = new TransferPlanner().prepare(spec, source, runId,
                    Instant.parse("2026-08-14T00:00:00Z"), TransferEventListener.NOOP);
        }
        return new TransferEngine().execute(prepared, () -> connected(sourceRoot),
                () -> connected(targetRoot), new InMemoryTransferCheckpointStore(),
                TransferEventListener.NOOP, new TransferControl());
    }

    private LocalSourcePlugin connected(Path root) {
        Configuration configuration = Configuration.newDefault();
        configuration.set("rootPath", root.toString());
        LocalSourcePlugin plugin = new LocalSourcePlugin();
        plugin.connect(configuration);
        return plugin;
    }
}
