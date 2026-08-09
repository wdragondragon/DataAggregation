package com.jdragon.aggregation.transfer.plugin;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import com.jdragon.aggregation.transfer.TransferFileSystemFactory;
import com.jdragon.aggregation.transfer.model.TransferEndpoint;

public class SourceTransferFileSystemFactory implements TransferFileSystemFactory {

    private final TransferEndpoint endpoint;

    public SourceTransferFileSystemFactory(TransferEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public TransferFileSystem open() throws Exception {
        AbstractPlugin plugin;
        try (PluginClassLoaderCloseable loader =
                     PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(
                             SourcePluginType.SOURCE, endpoint.plugin())) {
            plugin = loader.loadPlugin();
            if (!(plugin instanceof TransferFileSystem fileSystem)) {
                closeQuietly(plugin);
                throw new IllegalArgumentException("source plugin does not support file transfer: "
                        + endpoint.plugin());
            }
            try {
                if (!fileSystem.connect(Configuration.from(endpoint.config()))) {
                    closeQuietly(fileSystem);
                    throw new IllegalStateException("source plugin connection returned false: "
                            + endpoint.plugin());
                }
                return fileSystem;
            } catch (Exception e) {
                closeQuietly(fileSystem);
                throw e;
            }
        }
    }

    private void closeQuietly(Object value) {
        if (!(value instanceof AutoCloseable closeable)) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }
}
