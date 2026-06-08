package com.jdragon.aggregation.datasource.odps;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class OdpsUtils {
    public static Odps createOdps(BaseDataSourceDTO dataSource) {
        Account account = new AliyunAccount(dataSource.getUserName(), dataSource.getPassword());
        Odps odps = new Odps(account);
        odps.setEndpoint(dataSource.getHost());
        odps.setDefaultProject(dataSource.getDatabase());
        Map<String, String> map = buildGlobalSettings(dataSource.getExtraParams());
        if (!map.isEmpty()) {
            odps.setGlobalSettings(map);
        }
        return odps;
    }

    static Map<String, String> buildGlobalSettings(Map<String, ?> extraParams) {
        Map<String, String> globalSettings = new HashMap<>();
        Map<String, String> normalizedExtraParams = normalizeExtraParams(extraParams);
        if (normalizedExtraParams.isEmpty()) {
            return globalSettings;
        }
        for (Map.Entry<String, String> entry : normalizedExtraParams.entrySet()) {
            String key = entry.getKey();
            if (isConnectionMetadataKey(key) || StringUtils.isBlank(entry.getValue())) {
                continue;
            }
            globalSettings.put(key, entry.getValue());
        }
        return globalSettings;
    }

    public static Map<String, String> normalizeExtraParams(Map<String, ?> extraParams) {
        Map<String, String> normalized = new LinkedHashMap<>();
        if (extraParams == null || extraParams.isEmpty()) {
            return normalized;
        }

        flattenExtraParams("", extraParams, normalized);

        // Top-level scalar entries are treated as intentional literal keys and win over nested fallbacks.
        for (Map.Entry<String, ?> entry : extraParams.entrySet()) {
            Object value = entry.getValue();
            if (!(value instanceof Map)) {
                normalized.put(entry.getKey(), value == null ? null : String.valueOf(value));
            }
        }
        return normalized;
    }

    @SuppressWarnings("unchecked")
    private static void flattenExtraParams(String prefix, Map<String, ?> source, Map<String, String> target) {
        for (Map.Entry<String, ?> entry : source.entrySet()) {
            String key = entry.getKey();
            if (StringUtils.isBlank(key)) {
                continue;
            }
            String fullKey = StringUtils.isBlank(prefix) ? key : prefix + "." + key;
            Object value = entry.getValue();
            if (value instanceof Map) {
                flattenExtraParams(fullKey, (Map<String, ?>) value, target);
            } else {
                target.put(fullKey, value == null ? null : String.valueOf(value));
            }
        }
    }

    private static boolean isConnectionMetadataKey(String key) {
        return "projectEnv".equalsIgnoreCase(key)
                || "tunnelEndpoint".equalsIgnoreCase(key)
                || "dataworksEndpoint".equalsIgnoreCase(key)
                || "regionId".equalsIgnoreCase(key);
    }
}
