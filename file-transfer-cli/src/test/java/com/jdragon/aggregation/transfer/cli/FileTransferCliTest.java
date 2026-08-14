package com.jdragon.aggregation.transfer.cli;

import com.jdragon.aggregation.transfer.model.SourceSnapshot;
import com.jdragon.aggregation.transfer.model.TransferCheckpoint;
import com.jdragon.aggregation.transfer.plugin.FileTransferJobPlugin;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileTransferCliTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void validateWritesOneJsonLineAndReturnsSuccess() throws Exception {
        Path job = temporaryFolder.newFile("job.json").toPath();
        Files.writeString(job, validConfiguration(), StandardCharsets.UTF_8);
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        FileTransferCli cli = new FileTransferCli(new PrintStream(stdout),
                new PrintStream(stderr), new FileTransferJobPlugin());

        int exitCode = cli.run(new String[]{"validate", job.toString()});

        assertEquals(FileTransferCli.EXIT_SUCCESS, exitCode);
        assertTrue(stdout.toString(StandardCharsets.UTF_8).contains("VALIDATION_SUCCEEDED"));
        assertEquals("", stderr.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void missingArgumentsUseConfigurationExitCode() {
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        FileTransferCli cli = new FileTransferCli(new PrintStream(new ByteArrayOutputStream()),
                new PrintStream(stderr), new FileTransferJobPlugin());

        assertEquals(FileTransferCli.EXIT_CONFIG_ERROR, cli.run(new String[]{"run"}));
        assertTrue(stderr.toString(StandardCharsets.UTF_8).contains("usage:"));
    }

    @Test
    public void checkpointFileRoundTripsIdentityAndOffset() throws Exception {
        Path checkpointFile = temporaryFolder.newFile("checkpoint.json").toPath();
        Files.delete(checkpointFile);
        Path jobFile = temporaryFolder.newFile("checkpoint-job.json").toPath();
        JsonFileTransferCheckpointStore store = new JsonFileTransferCheckpointStore(checkpointFile);
        store.initialize("run-1", jobFile);
        TransferCheckpoint checkpoint = new TransferCheckpoint("run-1", "item-1", "source", "target",
                new SourceSnapshot("/source/a.txt", 12, 1234, "etag"), "/target/a.part", 5,
                TransferCheckpoint.CURRENT_SCHEMA_VERSION, "SHA-256", "BC-SHA256-ENCODED-V1",
                "encoded-state", 5,
                null, 0, 5, "head", 0, 5, "boundary",
                Instant.parse("2026-08-07T00:00:00Z"));
        store.save(checkpoint);

        JsonFileTransferCheckpointStore restored = new JsonFileTransferCheckpointStore(checkpointFile);

        assertEquals("run-1", restored.runId());
        assertEquals(jobFile.toAbsolutePath().normalize(), restored.jobFile());
        assertEquals(checkpoint, restored.load("run-1", "item-1").orElseThrow());
    }

    private String validConfiguration() {
        return "{\n"
                + "  \"schemaVersion\": 1,\n"
                + "  \"source\": {\"plugin\": \"local\", \"config\": {\"rootPath\": \"C:/source\"}},\n"
                + "  \"target\": {\"plugin\": \"local\", \"config\": {\"rootPath\": \"C:/target\"}},\n"
                + "  \"selection\": {\"rootPath\": \"C:/source\"},\n"
                + "  \"mapping\": {\"targetRootPath\": \"C:/target\"}\n"
                + "}";
    }
}
