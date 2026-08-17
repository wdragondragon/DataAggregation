package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.transfer.model.TransferCheckpoint;

import java.io.IOException;
import java.util.Optional;

public interface TransferCheckpointStore {

    Optional<TransferCheckpoint> load(String runId, String itemId) throws IOException;

    void save(TransferCheckpoint checkpoint) throws IOException;

    void delete(String runId, String itemId) throws IOException;
}
