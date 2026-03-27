package com.jdragon.aggregation.core.sortmerge;

import com.jdragon.aggregation.core.streaming.CompositeKey;
import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Getter
public class OrderedKey {

    private final List<String> fields;
    private final List<Object> values;
    private final String encoded;

    private OrderedKey(List<String> fields, List<Object> values) {
        this.fields = Collections.unmodifiableList(new ArrayList<String>(fields));
        this.values = Collections.unmodifiableList(new ArrayList<Object>(values));
        this.encoded = CompositeKey.fromRecord(toValueMap(), fields).asString();
    }

    public static OrderedKey fromRow(Map<String, Object> row, List<String> keyFields) {
        List<Object> values = new ArrayList<Object>();
        if (keyFields != null) {
            for (String field : keyFields) {
                values.add(row != null ? row.get(field) : null);
            }
        }
        return new OrderedKey(keyFields != null ? keyFields : Collections.<String>emptyList(), values);
    }

    public Map<String, Object> toValueMap() {
        return new java.util.LinkedHashMap<String, Object>() {
            {
                for (int i = 0; i < Math.min(fields.size(), values.size()); i++) {
                    put(fields.get(i), values.get(i));
                }
            }
        };
    }

    @Override
    public String toString() {
        return encoded;
    }
}
