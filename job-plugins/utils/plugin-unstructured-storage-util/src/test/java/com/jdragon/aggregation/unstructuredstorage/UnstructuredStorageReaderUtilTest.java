package com.jdragon.aggregation.unstructuredstorage;

import com.alibaba.fastjson.JSONArray;
import com.jdragon.aggregation.commons.util.FastJsonMemory;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

public class UnstructuredStorageReaderUtilTest {

    @Test
    public void missingConfiguredParentNodeShouldReturnNoRows() {
        ColumnEntry column = new ColumnEntry();
        column.setParentNode("data.items");
        column.setName("customer_id");
        column.setType("STRING");

        JSONArray rows = UnstructuredStorageReaderUtil.detailJson(
                new FastJsonMemory("{}"), Arrays.asList(column));

        assertTrue(rows.isEmpty());
    }

    @Test
    public void nullParentNodeShouldReadFromRoot() {
        ColumnEntry column = new ColumnEntry();
        column.setParentNode(null);
        column.setName("customer_id");
        column.setType("STRING");

        JSONArray rows = UnstructuredStorageReaderUtil.detailJson(
                new FastJsonMemory("{\"customer_id\":\"C108\"}"), Arrays.asList(column));

        assertEquals(1, rows.size());
        assertEquals("C108", rows.getJSONObject(0).getString("customer_id"));
    }
}
