package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.transfer.model.TransferCheckpoint;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryTransferCheckpointStore implements TransferCheckpointStore {

    private final ConcurrentMap<String, TransferCheckpoint> checkpoints = new ConcurrentHashMap<>();

    @Override
    public Optional<TransferCheckpoint> load(String runId, String itemId) {
        return Optional.ofNullable(checkpoints.get(key(runId, itemId)));
    }

    @Override
    public void save(TransferCheckpoint checkpoint) {
        checkpoints.put(key(checkpoint.runId(), checkpoint.itemId()), checkpoint);
    }

    @Override
    public void delete(String runId, String itemId) {
        checkpoints.remove(key(runId, itemId));
    }

    private String key(String runId, String itemId) {
        return runId + '\0' + itemId;
    }
}
