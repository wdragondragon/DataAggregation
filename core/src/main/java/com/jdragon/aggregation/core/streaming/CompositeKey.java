package com.jdragon.aggregation.core.streaming;

import com.alibaba.fastjson.JSON;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Composite key used by streaming consistency/fusion execution.
 */
public class CompositeKey {

    private static final String VERSION_2_PREFIX = "v2:";
    private static final String KEY_DELIMITER = "-|-";

    private final List<String> keyFields;
    private final List<Object> keyValues;
    private final String encodedValue;

    private CompositeKey(List<String> keyFields, List<Object> keyValues) {
        this.keyFields = Collections.unmodifiableList(new ArrayList<>(keyFields));
        this.keyValues = Collections.unmodifiableList(new ArrayList<>(keyValues));
        this.encodedValue = VERSION_2_PREFIX + JSON.toJSONString(this.keyValues);
    }

    public static CompositeKey fromRecord(Map<String, Object> row, List<String> keyFields) {
        if (keyFields == null || keyFields.isEmpty()) {
            return new CompositeKey(Collections.singletonList("key"), Collections.<Object>singletonList("default"));
        }

        List<Object> values = new ArrayList<>(keyFields.size());
        for (String keyField : keyFields) {
            Object value = row != null ? row.get(keyField) : null;
            values.add(value != null ? String.valueOf(value) : null);
        }
        return new CompositeKey(keyFields, values);
    }

    public static CompositeKey fromEncoded(String encodedValue, List<String> keyFields) {
        List<Object> values = new ArrayList<>();
        if (encodedValue != null && encodedValue.startsWith(VERSION_2_PREFIX)) {
            String payload = encodedValue.substring(VERSION_2_PREFIX.length());
            List<Object> decodedValues = JSON.parseArray(payload, Object.class);
            if (decodedValues != null) {
                values.addAll(decodedValues);
            }
        } else {
            values.addAll(decodeLegacyValues(encodedValue));
        }
        if (keyFields != null) {
            while (values.size() < keyFields.size()) {
                values.add(null);
            }
            return new CompositeKey(keyFields, values);
        }
        return new CompositeKey(Collections.singletonList("key"), values.isEmpty()
                ? Collections.<Object>singletonList(encodedValue)
                : values);
    }

    private static List<Object> decodeLegacyValues(String encodedValue) {
        List<Object> values = new ArrayList<>();
        if (encodedValue != null && !encodedValue.isEmpty()) {
            String[] parts = encodedValue.split("-\\|-", -1);
            Collections.addAll(values, parts);
        }
        return values;
    }

    public String asString() {
        return encodedValue;
    }

    public Map<String, Object> toValueMap() {
        Map<String, Object> values = new LinkedHashMap<>();
        for (int i = 0; i < Math.min(keyFields.size(), keyValues.size()); i++) {
            values.put(keyFields.get(i), keyValues.get(i));
        }
        return values;
    }

    public List<String> getKeyFields() {
        return keyFields;
    }

    public List<Object> getKeyValues() {
        return keyValues;
    }

    @Override
    public String toString() {
        return encodedValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(encodedValue);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CompositeKey)) {
            return false;
        }
        CompositeKey other = (CompositeKey) obj;
        return Objects.equals(encodedValue, other.encodedValue);
    }
}
