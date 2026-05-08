package com.jdragon.aggregation.core.streaming;

import org.junit.Test;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CompositeKeyTest {

    @Test
    public void shouldDistinguishNullFromStringNull() {
        CompositeKey nullKey = CompositeKey.fromRecord(row("id", null), Arrays.asList("id"));
        CompositeKey stringNullKey = CompositeKey.fromRecord(row("id", "NULL"), Arrays.asList("id"));

        assertNotEquals(nullKey.asString(), stringNullKey.asString());
        assertNull(CompositeKey.fromEncoded(nullKey.asString(), Arrays.asList("id")).toValueMap().get("id"));
        assertEquals("NULL", CompositeKey.fromEncoded(stringNullKey.asString(), Arrays.asList("id")).toValueMap().get("id"));
    }

    @Test
    public void shouldAvoidDelimiterCollisionForCompositeKeys() {
        Map<String, Object> left = new LinkedHashMap<String, Object>();
        left.put("a", "x");
        left.put("b", "y-|-z");

        Map<String, Object> right = new LinkedHashMap<String, Object>();
        right.put("a", "x-|-y");
        right.put("b", "z");

        assertNotEquals(
                CompositeKey.fromRecord(left, Arrays.asList("a", "b")).asString(),
                CompositeKey.fromRecord(right, Arrays.asList("a", "b")).asString()
        );
    }

    @Test
    public void shouldRoundTripVersionedEncodedValues() {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("a", "x-|-y");
        row.put("b", null);
        row.put("c", "NULL");

        CompositeKey key = CompositeKey.fromRecord(row, Arrays.asList("a", "b", "c"));
        CompositeKey decoded = CompositeKey.fromEncoded(key.asString(), Arrays.asList("a", "b", "c"));

        assertTrue(key.asString().startsWith("v2:"));
        assertEquals("x-|-y", decoded.toValueMap().get("a"));
        assertNull(decoded.toValueMap().get("b"));
        assertEquals("NULL", decoded.toValueMap().get("c"));
        assertEquals(key.asString(), decoded.asString());
    }

    private static Map<String, Object> row(String field, Object value) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put(field, value);
        return row;
    }
}
