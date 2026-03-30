package com.jdragon.aggregation.core.sortmerge;

import com.jdragon.aggregation.commons.util.Configuration;
import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Map;

@Data
public class AdaptiveMergeConfig {

    public enum OrderViolationAction {
        RECOVER_LOCAL,
        BUCKET,
        FAIL
    }

    public enum MemoryExceededAction {
        SPILL_OLDEST,
        BUCKET
    }

    private boolean enabled = true;
    private int pendingKeyThreshold = 4096;
    private int pendingMemoryMB = 256;
    private int overflowPartitionCount = 16;
    private int rebalancePartitionMultiplier = 4;
    private String overflowSpillPath;
    private boolean preferOrderedQuery = true;
    private boolean validateSourceOrder = true;
    private boolean localDisorderEnabled = true;
    private int localDisorderMaxGroups = 1024;
    private int localDisorderMaxMemoryMB = 64;
    private int maxSpillBytesMB = 512;
    private int minFreeDiskMB = 256;
    private OrderViolationAction onOrderViolation = OrderViolationAction.RECOVER_LOCAL;
    private MemoryExceededAction onMemoryExceeded = MemoryExceededAction.SPILL_OLDEST;
    private Map<String, OrderedKeyType> keyTypes = new LinkedHashMap<String, OrderedKeyType>();

    public static AdaptiveMergeConfig fromConfig(Configuration configuration,
                                                 int defaultMemoryLimitMB,
                                                 int defaultPartitionCount,
                                                 String defaultSpillPath) {
        AdaptiveMergeConfig config = new AdaptiveMergeConfig();
        config.setPendingMemoryMB(Math.max(64, defaultMemoryLimitMB / 2));
        config.setOverflowPartitionCount(Math.max(1, defaultPartitionCount));
        config.setOverflowSpillPath(defaultSpillPath);
        if (configuration == null) {
            return config;
        }

        config.setEnabled(configuration.getBool("enabled", true));
        config.setPendingKeyThreshold(configuration.getInt("pendingKeyThreshold", config.getPendingKeyThreshold()));
        config.setPendingMemoryMB(configuration.getInt("pendingMemoryMB", config.getPendingMemoryMB()));
        config.setOverflowPartitionCount(configuration.getInt("overflowPartitionCount", config.getOverflowPartitionCount()));
        config.setRebalancePartitionMultiplier(configuration.getInt(
                "rebalancePartitionMultiplier",
                config.getRebalancePartitionMultiplier()
        ));
        config.setOverflowSpillPath(configuration.getString("overflowSpillPath", config.getOverflowSpillPath()));
        config.setPreferOrderedQuery(configuration.getBool("preferOrderedQuery", true));
        config.setValidateSourceOrder(configuration.getBool("validateSourceOrder", true));
        config.setLocalDisorderEnabled(configuration.getBool("localDisorderEnabled", true));
        config.setLocalDisorderMaxGroups(configuration.getInt(
                "localDisorderMaxGroups",
                defaultLocalDisorderMaxGroups(config.getPendingKeyThreshold())
        ));
        config.setLocalDisorderMaxMemoryMB(configuration.getInt(
                "localDisorderMaxMemoryMB",
                defaultLocalDisorderMaxMemoryMB(config.getPendingMemoryMB())
        ));
        config.setMaxSpillBytesMB(configuration.getInt("maxSpillBytesMB", config.getMaxSpillBytesMB()));
        config.setMinFreeDiskMB(configuration.getInt("minFreeDiskMB", config.getMinFreeDiskMB()));

        String violationAction = configuration.getString("onOrderViolation", config.getOnOrderViolation().name());
        config.setOnOrderViolation(parseOrderViolationAction(violationAction, config.getOnOrderViolation()));

        String memoryAction = configuration.getString("onMemoryExceeded", config.getOnMemoryExceeded().name());
        config.setOnMemoryExceeded(parseMemoryExceededAction(memoryAction, config.getOnMemoryExceeded()));

        Configuration keyTypesConfig = configuration.getConfiguration("keyTypes");
        if (keyTypesConfig != null) {
            Map<String, Object> values = keyTypesConfig.getMap("");
            if (values != null) {
                for (Map.Entry<String, Object> entry : values.entrySet()) {
                    if (entry.getValue() == null) {
                        continue;
                    }
                    config.getKeyTypes().put(entry.getKey(), OrderedKeyType.valueOf(String.valueOf(entry.getValue()).trim().toUpperCase()));
                }
            }
        }
        return config;
    }

    public long getPendingMemoryBytes() {
        return Math.max(1, pendingMemoryMB) * 1024L * 1024L;
    }

    public long getLocalDisorderMaxMemoryBytes() {
        return Math.max(1, localDisorderMaxMemoryMB) * 1024L * 1024L;
    }

    public long getMaxSpillBytes() {
        return Math.max(1, maxSpillBytesMB) * 1024L * 1024L;
    }

    public long getMinFreeDiskBytes() {
        return Math.max(1, minFreeDiskMB) * 1024L * 1024L;
    }

    private static int defaultLocalDisorderMaxGroups(int pendingKeyThreshold) {
        return Math.min(1024, Math.max(64, Math.max(1, pendingKeyThreshold) / 4));
    }

    private static int defaultLocalDisorderMaxMemoryMB(int pendingMemoryMB) {
        return Math.max(16, Math.max(1, pendingMemoryMB) / 4);
    }

    private static OrderViolationAction parseOrderViolationAction(String value, OrderViolationAction defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return OrderViolationAction.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException ignored) {
            return defaultValue;
        }
    }

    private static MemoryExceededAction parseMemoryExceededAction(String value, MemoryExceededAction defaultValue) {
        if (value == null || value.trim().isEmpty()) {
            return defaultValue;
        }
        try {
            return MemoryExceededAction.valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException ignored) {
            return defaultValue;
        }
    }
}
