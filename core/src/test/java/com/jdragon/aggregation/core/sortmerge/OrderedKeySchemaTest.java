package com.jdragon.aggregation.core.sortmerge;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class OrderedKeySchemaTest {

    @Test
    public void compareUsesNumericOrderingForNumericLikeValues() {
        Map<String, OrderedKeyType> keyTypes = new HashMap<String, OrderedKeyType>();
        keyTypes.put("id", OrderedKeyType.NUMBER);
        OrderedKeySchema schema = new OrderedKeySchema(Arrays.asList("id"), keyTypes);

        OrderedKey key2 = OrderedKey.fromRow(Collections.<String, Object>singletonMap("id", "2"), Arrays.asList("id"));
        OrderedKey key10 = OrderedKey.fromRow(Collections.<String, Object>singletonMap("id", 10L), Arrays.asList("id"));

        assertTrue(schema.compare(key2, key10) < 0);
    }
}
