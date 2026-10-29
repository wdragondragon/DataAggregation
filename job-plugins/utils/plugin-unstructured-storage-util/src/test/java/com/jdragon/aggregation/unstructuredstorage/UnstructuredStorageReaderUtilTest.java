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

    @Test
    public void legacyReaderShouldHonorSourceNamesForArrayAndRootFields() {
        ColumnEntry id = new ColumnEntry();
        id.setParentNode("data.items");
        id.setName("detail_id");
        id.setSourceName("id");
        ColumnEntry root = new ColumnEntry();
        root.setParentNode("");
        root.setName("request_id");
        root.setSourceName("id");

        JSONArray rows = UnstructuredStorageReaderUtil.detailJson(new FastJsonMemory(
                "{\"id\":\"R1\",\"data\":{\"items\":[{\"id\":\"D1\"},{\"id\":\"D2\"}]}}"), Arrays.asList(id, root));

        assertEquals(2, rows.size());
        assertEquals("D1", rows.getJSONObject(0).getString("detail_id"));
        assertEquals("R1", rows.getJSONObject(1).getString("request_id"));
    }

    @Test
    public void wildcardPathsRequireExplicitRecordPath() {
        ColumnEntry column = new ColumnEntry();
        column.setName("id");
        column.setParentNode("data[*].items[*]");
        org.junit.Assert.assertThrows(IllegalArgumentException.class,
                () -> UnstructuredStorageReaderUtil.compileJsonRecordPath(null, Arrays.asList(column)));
    }
}
