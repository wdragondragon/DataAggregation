package com.jdragon.aggregation.transfer.cli;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.datasource.file.transfer.TransferFilePage;
import com.jdragon.aggregation.transfer.InMemoryTransferCheckpointStore;
import com.jdragon.aggregation.transfer.PreparedTransfer;
import com.jdragon.aggregation.transfer.TransferCheckpointStore;
import com.jdragon.aggregation.transfer.TransferControl;
import com.jdragon.aggregation.transfer.model.TransferEvent;
import com.jdragon.aggregation.transfer.model.TransferEventType;
import com.jdragon.aggregation.transfer.model.TransferResult;
import com.jdragon.aggregation.transfer.model.TransferRunStatus;
import com.jdragon.aggregation.transfer.plugin.FileTransferJobPlugin;

import java.io.File;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class FileTransferCli {

    public static final int EXIT_SUCCESS = 0;
    public static final int EXIT_CONFIG_ERROR = 2;
    public static final int EXIT_PARTIAL_SUCCESS = 3;
    public static final int EXIT_RUN_FAILED = 4;
    public static final int EXIT_CANCELED = 5;

    private final PrintStream output;
    private final PrintStream error;
    private final FileTransferJobPlugin plugin;
    private final JsonLineTransferEventWriter events;

    public FileTransferCli() {
        this(System.out, System.err, new FileTransferJobPlugin());
    }

    FileTransferCli(PrintStream output, PrintStream error, FileTransferJobPlugin plugin) {
        this.output = output;
        this.error = error;
        this.plugin = plugin;
        this.events = new JsonLineTransferEventWriter(output);
    }

    public static void main(String[] args) {
        System.exit(new FileTransferCli().run(args));
    }

    public int run(String[] args) {
        if (args == null || args.length < 2) {
            usage();
            return EXIT_CONFIG_ERROR;
        }
        try {
            return switch (args[0]) {
                case "validate" -> validate(Path.of(args[1]));
                case "browse" -> browse(Path.of(args[1]), slice(args, 2));
                case "plan" -> plan(Path.of(args[1]));
                case "run" -> execute(Path.of(args[1]), false);
                case "resume" -> resume(Path.of(args[1]));
                default -> {
                    usage();
                    yield EXIT_CONFIG_ERROR;
                }
            };
        } catch (IllegalArgumentException e) {
            error.println("configuration error: " + safeMessage(e));
            return EXIT_CONFIG_ERROR;
        } catch (Exception e) {
            error.println("file transfer failed: " + safeMessage(e));
            return EXIT_RUN_FAILED;
        }
    }

    private int validate(Path jobFile) {
        Configuration configuration = read(jobFile);
        plugin.validate(configuration);
        events.onEvent(new TransferEvent(TransferEventType.VALIDATION_SUCCEEDED, Instant.now(),
                configuration.getString("runId"), null, null, 0, 0, 0,
                "file transfer configuration is valid", null,
                Map.of("jobFile", jobFile.toAbsolutePath().normalize().toString())));
        return EXIT_SUCCESS;
    }

    private int browse(Path endpointFile, String[] options) throws Exception {
        Map<String, String> parsed = options(options);
        String path = requiredOption(parsed, "--path");
        String cursor = parsed.get("--cursor");
        int pageSize = Integer.parseInt(parsed.getOrDefault("--page-size", "200"));
        TransferFilePage page = plugin.browse(read(endpointFile), path, cursor, pageSize);
        List<Map<String, Object>> entries = new ArrayList<>();
        for (TransferFileEntry entry : page.entries()) {
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("path", entry.path());
            value.put("name", entry.name());
            value.put("directory", entry.directory());
            value.put("size", entry.size());
            value.put("modifiedTimeMillis", entry.modifiedTimeMillis());
            value.put("etag", entry.etag());
            entries.add(value);
        }
        events.onEvent(new TransferEvent(TransferEventType.BROWSE_RESULT, Instant.now(), null,
                null, null, 0, 0, 0, "file page loaded", null,
                Map.of("entries", entries, "nextCursor", page.nextCursor() == null ? "" : page.nextCursor(),
                        "truncated", page.truncated())));
        return EXIT_SUCCESS;
    }

    private int plan(Path jobFile) throws Exception {
        Configuration configuration = read(jobFile);
        ensureRunId(configuration);
        PreparedTransfer prepared = plugin.plan(configuration, events);
        return prepared.plan().items().isEmpty() ? EXIT_SUCCESS : EXIT_SUCCESS;
    }

    private int execute(Path jobFile, boolean resume) throws Exception {
        Configuration configuration = read(jobFile);
        String runId = ensureRunId(configuration);
        String checkpointPath = configuration.getString("runtime.checkpointFile");
        TransferCheckpointStore checkpointStore;
        if (checkpointPath == null || checkpointPath.isBlank()) {
            checkpointStore = new InMemoryTransferCheckpointStore();
        } else {
            Path resolvedCheckpoint = resolveRelative(jobFile, checkpointPath);
            JsonFileTransferCheckpointStore fileStore = new JsonFileTransferCheckpointStore(resolvedCheckpoint);
            if (!resume) {
                fileStore.initialize(runId, jobFile);
            }
            checkpointStore = fileStore;
        }
        TransferResult result = plugin.run(configuration, checkpointStore, events, new TransferControl());
        return exitCode(result.status());
    }

    private int resume(Path checkpointFile) throws Exception {
        JsonFileTransferCheckpointStore store = new JsonFileTransferCheckpointStore(checkpointFile);
        Path jobFile = store.jobFile();
        Configuration configuration = read(jobFile);
        if (store.runId() == null || store.runId().isBlank()) {
            throw new IllegalArgumentException("checkpoint file does not contain runId");
        }
        configuration.set("runId", store.runId());
        TransferResult result = plugin.run(configuration, store, events, new TransferControl());
        return exitCode(result.status());
    }

    private Configuration read(Path file) {
        if (!java.nio.file.Files.isRegularFile(file)) {
            throw new IllegalArgumentException("configuration file does not exist: " + file);
        }
        return Configuration.from(file.toFile());
    }

    private String ensureRunId(Configuration configuration) {
        String runId = configuration.getString("runId");
        if (runId == null || runId.isBlank()) {
            runId = UUID.randomUUID().toString();
            configuration.set("runId", runId);
        }
        return runId;
    }

    private Path resolveRelative(Path jobFile, String value) {
        Path path = Path.of(value);
        if (path.isAbsolute()) {
            return path;
        }
        Path parent = jobFile.toAbsolutePath().normalize().getParent();
        return (parent == null ? path : parent.resolve(path)).normalize();
    }

    private Map<String, String> options(String[] values) {
        Map<String, String> parsed = new LinkedHashMap<>();
        for (int index = 0; index < values.length; index++) {
            String option = values[index];
            if (!option.startsWith("--") || index + 1 >= values.length) {
                throw new IllegalArgumentException("invalid option: " + option);
            }
            parsed.put(option, values[++index]);
        }
        return parsed;
    }

    private String requiredOption(Map<String, String> options, String name) {
        String value = options.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("missing option " + name);
        }
        return value;
    }

    private int exitCode(TransferRunStatus status) {
        return switch (status) {
            case SUCCESS -> EXIT_SUCCESS;
            case PARTIAL_SUCCESS -> EXIT_PARTIAL_SUCCESS;
            case CANCELED -> EXIT_CANCELED;
            default -> EXIT_RUN_FAILED;
        };
    }

    private String[] slice(String[] values, int from) {
        if (from >= values.length) {
            return new String[0];
        }
        return java.util.Arrays.copyOfRange(values, from, values.length);
    }

    private String safeMessage(Throwable throwable) {
        return throwable.getMessage() == null || throwable.getMessage().isBlank()
                ? throwable.getClass().getSimpleName()
                : throwable.getMessage();
    }

    private void usage() {
        error.println("usage: file-transfer <validate|browse|plan|run|resume> <json-file> [options]");
    }
}
