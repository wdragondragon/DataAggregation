package com.jdragon.aggregation.transfer.cli;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.transfer.TransferCheckpointStore;
import com.jdragon.aggregation.transfer.model.SourceSnapshot;
import com.jdragon.aggregation.transfer.model.TransferCheckpoint;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class JsonFileTransferCheckpointStore implements TransferCheckpointStore {

    private final Path file;
    private final Map<String, TransferCheckpoint> checkpoints = new LinkedHashMap<>();
    private String runId;
    private String jobFile;

    public JsonFileTransferCheckpointStore(Path file) throws IOException {
        this.file = file.toAbsolutePath().normalize();
        loadFile();
    }

    public synchronized void initialize(String runId, Path jobFile) throws IOException {
        this.runId = runId;
        this.jobFile = jobFile.toAbsolutePath().normalize().toString();
        persist();
    }

    public synchronized String runId() {
        return runId;
    }

    public synchronized Path jobFile() {
        if (jobFile == null || jobFile.isBlank()) {
            throw new IllegalStateException("checkpoint file does not contain jobFile");
        }
        return Path.of(jobFile);
    }

    @Override
    public synchronized Optional<TransferCheckpoint> load(String runId, String itemId) {
        return Optional.ofNullable(checkpoints.get(key(runId, itemId)));
    }

    @Override
    public synchronized void save(TransferCheckpoint checkpoint) throws IOException {
        checkpoints.put(key(checkpoint.runId(), checkpoint.itemId()), checkpoint);
        persist();
    }

    @Override
    public synchronized void delete(String runId, String itemId) throws IOException {
        checkpoints.remove(key(runId, itemId));
        persist();
    }

    private void loadFile() throws IOException {
        if (!Files.exists(file)) {
            return;
        }
        JSONObject root = JSON.parseObject(Files.readString(file, StandardCharsets.UTF_8));
        runId = root.getString("runId");
        jobFile = root.getString("jobFile");
        JSONArray items = root.getJSONArray("checkpoints");
        if (items == null) {
            return;
        }
        for (int index = 0; index < items.size(); index++) {
            JSONObject value = items.getJSONObject(index);
            JSONObject snapshotValue = value.getJSONObject("sourceSnapshot");
            SourceSnapshot snapshot = new SourceSnapshot(snapshotValue.getString("path"),
                    snapshotValue.getLongValue("size"), snapshotValue.getLongValue("modifiedTimeMillis"),
                    snapshotValue.getString("etag"));
            TransferCheckpoint checkpoint = new TransferCheckpoint(value.getString("runId"),
                    value.getString("itemId"), value.getString("sourceIdentity"),
                    value.getString("targetIdentity"), snapshot, value.getString("temporaryPath"),
                    value.getLongValue("confirmedOffset"), Instant.parse(value.getString("updatedAt")));
            checkpoints.put(key(checkpoint.runId(), checkpoint.itemId()), checkpoint);
        }
    }

    private void persist() throws IOException {
        Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        JSONObject root = new JSONObject(true);
        root.put("schemaVersion", 1);
        root.put("runId", runId);
        root.put("jobFile", jobFile);
        JSONArray items = new JSONArray();
        for (TransferCheckpoint checkpoint : checkpoints.values()) {
            JSONObject value = new JSONObject(true);
            value.put("runId", checkpoint.runId());
            value.put("itemId", checkpoint.itemId());
            value.put("sourceIdentity", checkpoint.sourceIdentity());
            value.put("targetIdentity", checkpoint.targetIdentity());
            JSONObject snapshot = new JSONObject(true);
            snapshot.put("path", checkpoint.sourceSnapshot().path());
            snapshot.put("size", checkpoint.sourceSnapshot().size());
            snapshot.put("modifiedTimeMillis", checkpoint.sourceSnapshot().modifiedTimeMillis());
            snapshot.put("etag", checkpoint.sourceSnapshot().etag());
            value.put("sourceSnapshot", snapshot);
            value.put("temporaryPath", checkpoint.temporaryPath());
            value.put("confirmedOffset", checkpoint.confirmedOffset());
            value.put("updatedAt", checkpoint.updatedAt().toString());
            items.add(value);
        }
        root.put("checkpoints", items);
        Path temporary = file.resolveSibling(file.getFileName() + ".tmp");
        Files.writeString(temporary, root.toJSONString(), StandardCharsets.UTF_8);
        try {
            Files.move(temporary, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private String key(String runId, String itemId) {
        return runId + '\0' + itemId;
    }
}
