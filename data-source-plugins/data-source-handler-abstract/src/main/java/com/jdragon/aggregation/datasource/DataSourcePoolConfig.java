package com.jdragon.aggregation.datasource;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Per-datasource JDBC connection-pool settings.
 *
 * <p>The values deliberately live in datasource metadata instead of process
 * properties.  This keeps unrelated datasources isolated and avoids turning
 * a worker-wide setting into an accidental capacity control.</p>
 */
public final class DataSourcePoolConfig {

    public static final String MAXIMUM_POOL_SIZE = "poolMaximumSize";
    public static final String MINIMUM_IDLE = "poolMinimumIdle";
    public static final String CONNECTION_TIMEOUT_MS = "poolConnectionTimeoutMs";
    public static final String IDLE_TIMEOUT_MS = "poolIdleTimeoutMs";
    public static final String MAX_LIFETIME_MS = "poolMaxLifetimeMs";
    public static final String KEEPALIVE_TIME_MS = "poolKeepaliveTimeMs";

    public static final int DEFAULT_MAXIMUM_POOL_SIZE = 20;
    public static final int DEFAULT_MINIMUM_IDLE = 2;
    public static final long DEFAULT_CONNECTION_TIMEOUT_MS = 60000L;
    public static final long DEFAULT_IDLE_TIMEOUT_MS = 60000L;
    public static final long DEFAULT_MAX_LIFETIME_MS = 180000L;
    public static final long DEFAULT_KEEPALIVE_TIME_MS = 30000L;

    private static final List<String> PARAMETER_KEYS;

    static {
        List<String> keys = new ArrayList<String>();
        keys.add(MAXIMUM_POOL_SIZE);
        keys.add(MINIMUM_IDLE);
        keys.add(CONNECTION_TIMEOUT_MS);
        keys.add(IDLE_TIMEOUT_MS);
        keys.add(MAX_LIFETIME_MS);
        keys.add(KEEPALIVE_TIME_MS);
        PARAMETER_KEYS = Collections.unmodifiableList(keys);
    }

    private final int maximumPoolSize;
    private final int minimumIdle;
    private final long connectionTimeoutMs;
    private final long idleTimeoutMs;
    private final long maxLifetimeMs;
    private final long keepaliveTimeMs;

    private DataSourcePoolConfig(int maximumPoolSize,
                                 int minimumIdle,
                                 long connectionTimeoutMs,
                                 long idleTimeoutMs,
                                 long maxLifetimeMs,
                                 long keepaliveTimeMs) {
        this.maximumPoolSize = maximumPoolSize;
        this.minimumIdle = minimumIdle;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.idleTimeoutMs = idleTimeoutMs;
        this.maxLifetimeMs = maxLifetimeMs;
        this.keepaliveTimeMs = keepaliveTimeMs;
    }

    public static DataSourcePoolConfig defaults() {
        return new DataSourcePoolConfig(DEFAULT_MAXIMUM_POOL_SIZE,
                DEFAULT_MINIMUM_IDLE,
                DEFAULT_CONNECTION_TIMEOUT_MS,
                DEFAULT_IDLE_TIMEOUT_MS,
                DEFAULT_MAX_LIFETIME_MS,
                DEFAULT_KEEPALIVE_TIME_MS);
    }

    /**
     * Parses direct or nested datasource metadata. Unknown keys are ignored so
     * existing plugin-specific parameters remain compatible.
     */
    public static DataSourcePoolConfig from(Map<String, ?> parameters) {
        Map<String, ?> values = parameters == null
                ? Collections.<String, Object>emptyMap() : parameters;
        int maximumPoolSize = parseInt(values, MAXIMUM_POOL_SIZE, DEFAULT_MAXIMUM_POOL_SIZE,
                1, 1000);
        Object configuredMinimumIdle = value(values, MINIMUM_IDLE);
        int minimumIdle = configuredMinimumIdle == null
                ? Math.min(DEFAULT_MINIMUM_IDLE, maximumPoolSize)
                : parseIntValue(MINIMUM_IDLE, configuredMinimumIdle, 0, maximumPoolSize);
        long connectionTimeoutMs = parseLong(values, CONNECTION_TIMEOUT_MS,
                DEFAULT_CONNECTION_TIMEOUT_MS, 250L, 600000L, false);
        long idleTimeoutMs = parseLong(values, IDLE_TIMEOUT_MS,
                DEFAULT_IDLE_TIMEOUT_MS, 10000L, 1800000L, true);
        long maxLifetimeMs = parseLong(values, MAX_LIFETIME_MS,
                DEFAULT_MAX_LIFETIME_MS, 30000L, 3600000L, true);
        long keepaliveTimeMs = parseLong(values, KEEPALIVE_TIME_MS,
                DEFAULT_KEEPALIVE_TIME_MS, 30000L, 1800000L, true);
        if (maxLifetimeMs > 0L && keepaliveTimeMs > 0L && keepaliveTimeMs >= maxLifetimeMs) {
            throw new IllegalArgumentException(KEEPALIVE_TIME_MS
                    + " must be less than " + MAX_LIFETIME_MS + " when both are enabled");
        }
        return new DataSourcePoolConfig(maximumPoolSize, minimumIdle, connectionTimeoutMs,
                idleTimeoutMs, maxLifetimeMs, keepaliveTimeMs);
    }

    /**
     * Resolves a datasource metadata map where pool settings may be stored in
     * the legacy nested {@code extraParams} map or as first-class fields.
     * First-class fields take precedence.
     */
    public static DataSourcePoolConfig fromDatasourceMetadata(Map<String, ?> metadata) {
        Map<String, Object> merged = new LinkedHashMap<String, Object>();
        if (metadata != null) {
            Object nested = value(metadata, "extraParams");
            copyNestedParameters(nested, merged);
            copyDirectParameters(metadata, merged);
        }
        return from(merged);
    }

    /**
     * Resolves the datasource-level pool switch using the same tolerant value
     * rules as the Studio metadata editor. A missing or blank switch keeps
     * the historical pooled behavior; an explicit false/zero/off disables
     * pooling. This method deliberately has no process-wide fallback or
     * worker setting: the decision belongs to one datasource definition.
     */
    public static boolean isEnabled(Map<String, ?> metadata) {
        Object configured = value(metadata, "usePool");
        if (configured == null && metadata != null) {
            // A few legacy task payloads kept the switch in extraParams. Keep
            // that form readable while allowing the first-class field to win
            // whenever both representations are present.
            Object nested = value(metadata, "extraParams");
            try {
                configured = value(nestedParameters(nested), "usePool");
            } catch (IllegalArgumentException ignored) {
                // Pool metadata validation reports malformed extraParams when
                // pooling is enabled; a switch lookup alone should retain the
                // historical pooled default instead of changing behavior.
                configured = null;
            }
        }
        if (configured == null) {
            return true;
        }
        if (configured instanceof Boolean) {
            return ((Boolean) configured).booleanValue();
        }
        if (configured instanceof Number) {
            return ((Number) configured).intValue() != 0;
        }
        String normalized = String.valueOf(configured).trim().toLowerCase(Locale.ENGLISH);
        if (normalized.isEmpty()) {
            return true;
        }
        return "true".equals(normalized) || "1".equals(normalized)
                || "yes".equals(normalized) || "on".equals(normalized);
    }

    public static boolean hasConfiguredParameters(Map<String, ?> parameters) {
        if (parameters == null || parameters.isEmpty()) {
            return false;
        }
        for (String key : PARAMETER_KEYS) {
            if (value(parameters, key) != null) {
                return true;
            }
        }
        Object nested = value(parameters, "extraParams");
        Map<String, ?> nestedParameters = nestedParameters(nested);
        return !nestedParameters.isEmpty() && hasConfiguredParameters(nestedParameters);
    }

    /**
     * Copies an {@code extraParams} value supplied either as a Map (the normal
     * Jackson form) or as a JSON object string (legacy DTO form).  Invalid
     * object strings are rejected instead of silently dropping pool settings.
     */
    public static void copyNestedParameters(Object configured, Map<String, ?> target) {
        if (configured == null || target == null) {
            return;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> writable = (Map<String, Object>) target;
        for (Map.Entry<String, ?> entry : nestedParameters(configured).entrySet()) {
            writable.put(entry.getKey(), entry.getValue());
        }
    }

    /** Copies nested parameters into the string-valued map used by plugin DTOs. */
    public static void copyNestedStringParameters(Object configured, Map<String, String> target) {
        if (configured == null || target == null) {
            return;
        }
        for (Map.Entry<String, ?> entry : nestedParameters(configured).entrySet()) {
            if (entry.getValue() != null) {
                target.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
    }

    /** Copies only the six direct first-class fields using canonical names. */
    public static void copyDirectParameters(Map<String, ?> source, Map<String, ?> target) {
        if (source == null || target == null) {
            return;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> writable = (Map<String, Object>) target;
        for (String key : PARAMETER_KEYS) {
            Object configured = value(source, key);
            if (configured != null) {
                writable.put(key, configured);
            }
        }
    }

    /**
     * Copies first-class fields into the string-valued map used by legacy
     * plugin DTOs.  Values are deliberately converted to strings here rather
     * than relying on a heap-polluting cast from {@code Map<String, Object>}
     * to {@code Map<String, String>}.
     */
    public static void copyDirectStringParameters(Map<String, ?> source, Map<String, String> target) {
        if (source == null || target == null) {
            return;
        }
        for (String key : PARAMETER_KEYS) {
            Object configured = value(source, key);
            if (configured != null) {
                target.put(key, String.valueOf(configured));
            }
        }
    }

    /** Returns a canonical string map suitable for a plugin DTO extraParams map. */
    public static Map<String, String> canonicalParameters(Map<String, ?> parameters) {
        DataSourcePoolConfig config = from(parameters);
        Map<String, String> result = new LinkedHashMap<String, String>();
        result.put(MAXIMUM_POOL_SIZE, String.valueOf(config.maximumPoolSize));
        result.put(MINIMUM_IDLE, String.valueOf(config.minimumIdle));
        result.put(CONNECTION_TIMEOUT_MS, String.valueOf(config.connectionTimeoutMs));
        result.put(IDLE_TIMEOUT_MS, String.valueOf(config.idleTimeoutMs));
        result.put(MAX_LIFETIME_MS, String.valueOf(config.maxLifetimeMs));
        result.put(KEEPALIVE_TIME_MS, String.valueOf(config.keepaliveTimeMs));
        return result;
    }

    /** Stable, non-sensitive segment for cache-key partitioning. */
    public String cacheKey() {
        return maximumPoolSize + ":" + minimumIdle + ":" + connectionTimeoutMs + ":"
                + idleTimeoutMs + ":" + maxLifetimeMs + ":" + keepaliveTimeMs;
    }

    public boolean isDefault() {
        return maximumPoolSize == DEFAULT_MAXIMUM_POOL_SIZE
                && minimumIdle == DEFAULT_MINIMUM_IDLE
                && connectionTimeoutMs == DEFAULT_CONNECTION_TIMEOUT_MS
                && idleTimeoutMs == DEFAULT_IDLE_TIMEOUT_MS
                && maxLifetimeMs == DEFAULT_MAX_LIFETIME_MS
                && keepaliveTimeMs == DEFAULT_KEEPALIVE_TIME_MS;
    }

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public int getMinimumIdle() {
        return minimumIdle;
    }

    public long getConnectionTimeoutMs() {
        return connectionTimeoutMs;
    }

    public long getIdleTimeoutMs() {
        return idleTimeoutMs;
    }

    public long getMaxLifetimeMs() {
        return maxLifetimeMs;
    }

    public long getKeepaliveTimeMs() {
        return keepaliveTimeMs;
    }

    private static int parseInt(Map<String, ?> values, String key, int defaultValue, int min, int max) {
        Object configured = value(values, key);
        return configured == null ? defaultValue : parseIntValue(key, configured, min, max);
    }

    private static int parseIntValue(String key, Object configured, int min, int max) {
        long parsed = parseNumber(key, configured);
        if (parsed < min || parsed > max) {
            throw new IllegalArgumentException(key + " must be between " + min + " and " + max);
        }
        return (int) parsed;
    }

    private static long parseLong(Map<String, ?> values, String key, long defaultValue,
                                  long min, long max, boolean allowZero) {
        Object configured = value(values, key);
        if (configured == null) {
            return defaultValue;
        }
        long parsed = parseNumber(key, configured);
        if (allowZero && parsed == 0L) {
            return 0L;
        }
        if (parsed < min || parsed > max) {
            String range = allowZero ? "0 or between " + min + " and " + max
                    : "between " + min + " and " + max;
            throw new IllegalArgumentException(key + " must be " + range);
        }
        return parsed;
    }

    private static long parseNumber(String key, Object configured) {
        if (configured instanceof Number) {
            double numeric = ((Number) configured).doubleValue();
            long value = ((Number) configured).longValue();
            if (Double.isNaN(numeric) || Double.isInfinite(numeric) || numeric != value) {
                throw new IllegalArgumentException(key + " must be an integer");
            }
            return value;
        }
        String text = String.valueOf(configured).trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException(key + " must be an integer");
        }
        try {
            return Long.parseLong(text);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(key + " must be an integer", exception);
        }
    }

    private static Object value(Map<String, ?> values, String key) {
        if (values == null) {
            return null;
        }
        Object exact = values.get(key);
        if (exact != null || values.containsKey(key)) {
            return exact;
        }
        for (Map.Entry<String, ?> entry : values.entrySet()) {
            if (entry.getKey() != null && key.equalsIgnoreCase(entry.getKey().trim())) {
                return entry.getValue();
            }
        }
        return null;
    }

    private static Map<String, ?> nestedParameters(Object configured) {
        if (configured instanceof Map<?, ?>) {
            Map<String, Object> result = new LinkedHashMap<String, Object>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) configured).entrySet()) {
                if (entry.getKey() != null) {
                    result.put(String.valueOf(entry.getKey()), entry.getValue());
                }
            }
            return result;
        }
        if (configured instanceof String) {
            String text = ((String) configured).trim();
            if (text.isEmpty()) {
                return Collections.<String, Object>emptyMap();
            }
            if (!text.startsWith("{")) {
                return Collections.<String, Object>emptyMap();
            }
            try {
                Map<String, Object> parsed = JSON.parseObject(text,
                        new TypeReference<LinkedHashMap<String, Object>>() { });
                return parsed == null ? Collections.<String, Object>emptyMap() : parsed;
            } catch (RuntimeException exception) {
                throw new IllegalArgumentException("extraParams must be a JSON object", exception);
            }
        }
        return Collections.<String, Object>emptyMap();
    }
}
