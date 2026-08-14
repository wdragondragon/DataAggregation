package com.jdragon.aggregation.transfer.plugin;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.transfer.model.ConflictPolicy;
import com.jdragon.aggregation.transfer.model.CheckpointRecoveryMode;
import com.jdragon.aggregation.transfer.model.SourceSuccessAction;
import com.jdragon.aggregation.transfer.model.TransferEndpoint;
import com.jdragon.aggregation.transfer.model.TransferMapping;
import com.jdragon.aggregation.transfer.model.TransferPolicy;
import com.jdragon.aggregation.transfer.model.TransferRuntimeOptions;
import com.jdragon.aggregation.transfer.model.TransferSelection;
import com.jdragon.aggregation.transfer.model.TransferSpec;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TransferConfigurationMapper {

    public TransferSpec map(Configuration configuration) {
        if (configuration == null) {
            throw new IllegalArgumentException("file transfer configuration is required");
        }
        int schemaVersion = configuration.getInt("schemaVersion", 1);
        TransferEndpoint source = endpoint(configuration, "source");
        TransferEndpoint target = endpoint(configuration, "target");
        TransferSelection selection = new TransferSelection(
                required(configuration, "selection.rootPath"),
                strings(configuration, "selection.paths"),
                configuration.getBool("selection.recursive", true),
                strings(configuration, "selection.includeGlobs"),
                configuration.getString("selection.includeRegex"),
                strings(configuration, "selection.excludeGlobs"),
                configuration.getLong("selection.minSize"),
                configuration.getLong("selection.maxSize"),
                configuration.getLong("selection.modifiedAfterMillis"),
                configuration.getLong("selection.modifiedBeforeMillis"),
                configuration.getInt("selection.maxFiles", 100_000));
        TransferMapping mapping = new TransferMapping(
                required(configuration, "mapping.targetRootPath"),
                configuration.getBool("mapping.preserveRelativePath", true),
                configuration.getString("mapping.targetPathTemplate"));
        TransferPolicy policy = new TransferPolicy(
                enumValue(ConflictPolicy.class,
                        configuration.getString("policy.conflictPolicy", ConflictPolicy.FAIL.name())),
                configuration.getString("policy.checksumAlgorithm", "SHA-256"),
                enumValue(SourceSuccessAction.class,
                        configuration.getString("policy.sourceSuccessAction", SourceSuccessAction.KEEP.name())),
                configuration.getString("policy.sourceBackupRootPath"));
        TransferRuntimeOptions defaults = TransferRuntimeOptions.defaults();
        TransferRuntimeOptions runtime = new TransferRuntimeOptions(
                configuration.getInt("runtime.concurrency", defaults.concurrency()),
                configuration.getInt("runtime.maxRetries", defaults.maxRetries()),
                longs(configuration, "runtime.retryBackoffMillis", defaults.retryBackoffMillis()),
                configuration.getLong("runtime.chunkSizeBytes", defaults.chunkSizeBytes()),
                configuration.getLong("runtime.checkpointIntervalMillis",
                        defaults.checkpointIntervalMillis()),
                configuration.getLong("runtime.maxBytesPerSecond", defaults.maxBytesPerSecond()),
                enumValue(CheckpointRecoveryMode.class, configuration.getString(
                        "runtime.checkpointRecoveryMode", defaults.checkpointRecoveryMode().name())));
        return new TransferSpec(schemaVersion, source, target, selection, mapping, policy, runtime,
                configuration.getString("timeZone", "UTC"), parameters(configuration));
    }

    public TransferEndpoint endpoint(Configuration configuration, String path) {
        String plugin = configuration.getString(path + ".plugin",
                configuration.getString(path + ".type"));
        if (plugin == null || plugin.isBlank()) {
            throw new IllegalArgumentException(path + ".plugin is required");
        }
        String identity = configuration.getString(path + ".identity", plugin);
        Map<String, Object> config = configuration.getMap(path + ".config", Map.of());
        return new TransferEndpoint(plugin, identity, config);
    }

    private String required(Configuration configuration, String path) {
        String value = configuration.getString(path);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(path + " is required");
        }
        return value;
    }

    private List<String> strings(Configuration configuration, String path) {
        List<Object> values = configuration.getList(path, List.of());
        List<String> strings = new ArrayList<>();
        for (Object value : values) {
            if (value != null) {
                strings.add(String.valueOf(value));
            }
        }
        return strings;
    }

    private List<Long> longs(Configuration configuration, String path, List<Long> defaults) {
        List<Object> values = configuration.getList(path);
        if (values == null || values.isEmpty()) {
            return defaults;
        }
        List<Long> longs = new ArrayList<>();
        for (Object value : values) {
            if (value instanceof Number number) {
                longs.add(number.longValue());
            } else {
                longs.add(Long.parseLong(String.valueOf(value)));
            }
        }
        return longs;
    }

    private Map<String, String> parameters(Configuration configuration) {
        Map<String, Object> values = configuration.getMap("parameters", Map.of());
        Map<String, String> parameters = new LinkedHashMap<>();
        values.forEach((key, value) -> {
            if (value != null) {
                parameters.put(key, String.valueOf(value));
            }
        });
        return parameters;
    }

    private <E extends Enum<E>> E enumValue(Class<E> type, String value) {
        try {
            return Enum.valueOf(type, value.trim().replace('-', '_').toUpperCase());
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("invalid " + type.getSimpleName() + ": " + value, e);
        }
    }
}
