package com.jdragon.aggregation.transfer.plugin;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.transfer.TransferFilePage;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeSession;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import com.jdragon.aggregation.transfer.InMemoryTransferCheckpointStore;
import com.jdragon.aggregation.transfer.PreparedTransfer;
import com.jdragon.aggregation.transfer.TransferCheckpointStore;
import com.jdragon.aggregation.transfer.TransferControl;
import com.jdragon.aggregation.transfer.TransferEngine;
import com.jdragon.aggregation.transfer.TransferEventListener;
import com.jdragon.aggregation.transfer.TransferPlanner;
import com.jdragon.aggregation.transfer.model.TransferEndpoint;
import com.jdragon.aggregation.transfer.model.TransferResult;
import com.jdragon.aggregation.transfer.model.TransferSpec;

import java.time.Instant;
import java.util.UUID;

public class FileTransferJobPlugin extends AbstractPlugin {

    private final TransferConfigurationMapper configurationMapper = new TransferConfigurationMapper();

    public TransferSpec validate(Configuration configuration) {
        return configurationMapper.map(configuration);
    }

    public TransferFilePage browse(Configuration endpointConfiguration, String path,
                                   String cursor, int pageSize) throws Exception {
        TransferEndpoint endpoint = configurationMapper.endpoint(endpointConfiguration, "endpoint");
        try (PluginRuntimeSession session = PluginRuntimeSession.open();
             TransferFileSystem fileSystem = new SourceTransferFileSystemFactory(endpoint).open()) {
            return fileSystem.listPage(path, cursor, pageSize);
        }
    }

    public PreparedTransfer plan(Configuration configuration, TransferEventListener listener) throws Exception {
        TransferSpec spec = configurationMapper.map(configuration);
        String runId = configuration.getString("runId", UUID.randomUUID().toString());
        Instant plannedAt = parsePlannedAt(configuration.getString("plannedAt"));
        try (PluginRuntimeSession session = PluginRuntimeSession.open();
             TransferFileSystem source = new SourceTransferFileSystemFactory(spec.source()).open();
             TransferFileSystem target = new SourceTransferFileSystemFactory(spec.target()).open()) {
            return new TransferPlanner().prepare(spec, source, target, runId, plannedAt, listener);
        }
    }

    public TransferResult run(Configuration configuration,
                              TransferCheckpointStore checkpointStore,
                              TransferEventListener listener,
                              TransferControl control) throws Exception {
        TransferSpec spec = configurationMapper.map(configuration);
        String runId = configuration.getString("runId", UUID.randomUUID().toString());
        Instant plannedAt = parsePlannedAt(configuration.getString("plannedAt"));
        TransferEventListener events = listener == null ? TransferEventListener.NOOP : listener;
        try (PluginRuntimeSession session = PluginRuntimeSession.open()) {
            SourceTransferFileSystemFactory sourceFactory = new SourceTransferFileSystemFactory(spec.source());
            SourceTransferFileSystemFactory targetFactory = new SourceTransferFileSystemFactory(spec.target());
            PreparedTransfer prepared;
            try (TransferFileSystem source = sourceFactory.open();
                 TransferFileSystem target = targetFactory.open()) {
                prepared = new TransferPlanner().prepare(spec, source, target, runId, plannedAt, events);
            }
            TransferEngine engine = new TransferEngine(session::bind);
            return engine.execute(prepared, sourceFactory, targetFactory,
                    checkpointStore == null ? new InMemoryTransferCheckpointStore() : checkpointStore,
                    events, control == null ? new TransferControl() : control);
        }
    }

    private Instant parsePlannedAt(String value) {
        return value == null || value.isBlank() ? Instant.now() : Instant.parse(value);
    }
}
