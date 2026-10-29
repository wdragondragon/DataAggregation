package com.jdragon.aggregation.commons.util.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonRecordExtractorTest {

    @Test
    public void nestedArraysExpandDepthFirstAndInheritParentFields() {
        String response = "{\"data1\":["
                + "{\"orderNo\":\"A\",\"data2\":[{\"itemNo\":1},{\"itemNo\":2}]},"
                + "{\"orderNo\":\"B\",\"data2\":[{\"itemNo\":3}]}]}";
        JsonExtractionPlan plan = JsonRecordExtractor.compile("data1[*].data2[*]", Arrays.asList(
                new JsonFieldProjection("orderNo", null, "data1[*]"),
                new JsonFieldProjection("itemNo", null, "data1[*].data2[*]")));

        JsonExtractionResult result = plan.extract(JSON.parseObject(response), 0);

        assertEquals(2, result.getSourceRowCount());
        assertEquals(3, result.getRows().size());
        assertEquals("A", result.getRows().getJSONObject(0).getString("orderNo"));
        assertEquals(1, result.getRows().getJSONObject(0).getIntValue("itemNo"));
        assertEquals("A", result.getRows().getJSONObject(1).getString("orderNo"));
        assertEquals("B", result.getRows().getJSONObject(2).getString("orderNo"));
    }

    @Test
    public void outputOrderAndNullKeysArePreserved() {
        JsonExtractionPlan plan = JsonRecordExtractor.compile("data[*]", Arrays.asList(
                new JsonFieldProjection("first", "absent", "data[*]"),
                new JsonFieldProjection("second", "value", "data[*]")));
        JsonExtractionResult result = plan.extract(JSON.parseObject("{\"data\":[{\"value\":\"x\"}] }"), 1);
        JSONObject row = result.getRows().getJSONObject(0);

        assertEquals(Arrays.asList("first", "second"), Arrays.asList(row.keySet().toArray()));
        assertTrue(row.containsKey("first"));
        assertNull(row.get("first"));
        assertEquals("x", row.getString("second"));
    }

    @Test
    public void maxRowsLimitsOutputButNotFirstWildcardSourceCount() {
        JsonExtractionPlan plan = JsonRecordExtractor.compile("data[*].children[*]", Collections.singletonList(
                new JsonFieldProjection("id", null, "data[*].children[*]")));
        JsonExtractionResult result = plan.extract(JSON.parseObject(
                "{\"data\":[{\"children\":[{\"id\":1},{\"id\":2}]},"
                        + "{\"children\":[{\"id\":3}]}]}"), 1);

        assertEquals(2, result.getSourceRowCount());
        assertEquals(1, result.getRows().size());
        assertEquals(1, result.getRows().getJSONObject(0).getIntValue("id"));
    }

    @Test
    public void recordPathKeepsChildGranularityWhenOnlyParentFieldsAreSelected() {
        JsonExtractionPlan plan = JsonRecordExtractor.compile("orders[*].items[*]", Arrays.asList(
                new JsonFieldProjection("requestId", null, "$"),
                new JsonFieldProjection("orderId", "id", "orders[*]")));
        JsonExtractionResult result = plan.extract(JSON.parseObject(
                "{\"requestId\":\"req1\",\"orders\":[{\"id\":\"A\",\"items\":[{},{}]},"
                        + "{\"id\":\"B\",\"items\":[{}]}]}"), 0);

        assertEquals(3, result.getRows().size());
        assertEquals("A", result.getRows().getJSONObject(0).getString("orderId"));
        assertEquals("A", result.getRows().getJSONObject(1).getString("orderId"));
        assertEquals("B", result.getRows().getJSONObject(2).getString("orderId"));
        assertEquals("req1", result.getRows().getJSONObject(2).getString("requestId"));
    }

    @Test
    public void nullMissingAndEmptyChildArraysProduceNoRowsButKeepOuterCount() {
        JsonExtractionPlan plan = JsonRecordExtractor.compile("orders[*].items[*]", Collections.singletonList(
                new JsonFieldProjection("id", null, "orders[*].items[*]")));
        JsonExtractionResult result = plan.extract(JSON.parseObject(
                "{\"orders\":[{\"items\":[]},{\"items\":null},{},{\"items\":[{}]}]}"), 0);

        assertEquals(4, result.getSourceRowCount());
        assertEquals(1, result.getRows().size());
        assertTrue(result.getRows().getJSONObject(0).containsKey("id"));
        assertNull(result.getRows().getJSONObject(0).get("id"));
        assertEquals(0, plan.extract(null, 0).getSourceRowCount());
    }

    @Test
    public void fixedProjectionIndexCanBeCombinedWithBoundParentWildcard() {
        JsonExtractionPlan plan = JsonRecordExtractor.compile("data[*].children[*]", Collections.singletonList(
                new JsonFieldProjection("firstChild", "id", "data[*].children[0]")));
        JsonExtractionResult result = plan.extract(JSON.parseObject(
                "{\"data\":[{\"children\":[{\"id\":\"x\"},{\"id\":\"y\"}]},"
                        + "{\"children\":[{\"id\":\"z\"}]}]}"), 0);

        assertEquals(3, result.getRows().size());
        assertEquals("x", result.getRows().getJSONObject(0).getString("firstChild"));
        assertEquals("x", result.getRows().getJSONObject(1).getString("firstChild"));
        assertEquals("z", result.getRows().getJSONObject(2).getString("firstChild"));
    }

    @Test
    public void fixedIndexOutOfRangeProducesNoRecordAndEmptyArrayHasZeroCount() {
        JsonExtractionPlan fixed = JsonRecordExtractor.compile("data[2]", Collections.emptyList());
        JsonExtractionResult fixedResult = fixed.extract(JSON.parseObject("{\"data\":[{}]}"), 0);
        assertEquals(0, fixedResult.getRows().size());
        assertEquals(0, fixedResult.getSourceRowCount());

        JsonExtractionPlan wildcard = JsonRecordExtractor.compile("data[*]", Collections.emptyList());
        JsonExtractionResult emptyResult = wildcard.extract(JSON.parseObject("{\"data\":[]}"), 0);
        assertEquals(0, emptyResult.getRows().size());
        assertEquals(0, emptyResult.getSourceRowCount());
    }

    @Test
    public void rootObjectAndRootArrayAreSupported() {
        JsonExtractionPlan objectPlan = JsonRecordExtractor.compile("$", Collections.singletonList(
                new JsonFieldProjection("id", null, "$")));
        assertEquals("A", objectPlan.extract(JSON.parseObject("{\"id\":\"A\"}"), 0)
                .getRows().getJSONObject(0).getString("id"));

        JsonExtractionPlan arrayPlan = JsonRecordExtractor.compile("$[*]", Collections.singletonList(
                new JsonFieldProjection("id", null, "$[*]")));
        assertEquals(2, arrayPlan.extract(JSON.parseArray("[{\"id\":\"A\"},{\"id\":\"B\"}]"), 0)
                .getRows().size());
    }

    @Test
    public void duplicateExactProjectionIsAllowedButConflictingOutputNameIsRejected() {
        JsonExtractionPlan plan = JsonRecordExtractor.compile("data[*]", Arrays.asList(
                new JsonFieldProjection("id", "source", "data[*]"),
                new JsonFieldProjection("id", "source", "data[*]")));
        JSONObject row = plan.extract(JSON.parseObject("{\"data\":[{\"source\":7}]}"), 0)
                .getRows().getJSONObject(0);
        assertEquals(1, row.size());
        assertEquals(7, row.getIntValue("id"));
        assertCompileFails("data[*]", Arrays.asList(
                new JsonFieldProjection("id", "sourceA", "data[*]"),
                new JsonFieldProjection("id", "sourceB", "data[*]")), "duplicate output name");
    }

    @Test
    public void unboundAndSiblingWildcardsAreRejected() {
        assertCompileFails("data[*].children[*]", Collections.singletonList(
                new JsonFieldProjection("other", null, "data[*].other[*]")), "not a prefix");
        assertCompileFails("data[*].children[*]", Collections.singletonList(
                new JsonFieldProjection("other", null, "data[*].children[*].details[*]")), "unbound wildcard");
    }

    @Test
    public void wildcardRequiresArrayAtRuntimeAndMissingBranchIsEmpty() {
        JsonExtractionPlan plan = JsonRecordExtractor.compile("data[*].children[*]", Collections.emptyList());
        assertEquals(0, plan.extract(JSON.parseObject("{}"), 0).getRows().size());
        try {
            plan.extract(JSON.parseObject("{\"data\":{}}"), 0);
            fail("expected a type error for a non-array wildcard node");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("expected array"));
        }
    }

    @Test
    public void unsupportedPathSyntaxIsRejected() {
        for (String path : Arrays.asList("data[?(@.id > 1)]", "$..data", "data[-1]", "data['id']",
                "data[1,2]", "data.", "$.data.[*]", "data[]", "$data", "$.")) {
            assertCompileFails(path, Collections.emptyList(), "unsupported");
        }
        assertCompileFails(" ", Collections.emptyList(), "must not be blank");
    }

    @Test
    public void unicodeAndHyphenPropertiesAndLiteralSourceNamesAreSupported() {
        JsonExtractionPlan plan = JsonRecordExtractor.compile("$.响应.商品-列表[*]", Collections.singletonList(
                new JsonFieldProjection("output", "name.with[syntax]", "响应.商品-列表[*]")));
        JsonExtractionResult result = plan.extract(JSON.parseObject(
                "{\"响应\":{\"商品-列表\":[{\"name.with[syntax]\":\"值\"}]}}"), 0);
        assertEquals("值", result.getRows().getJSONObject(0).getString("output"));
    }

    @Test
    public void fixedDriverPrefixBindsOnlyItsOwnWildcardAndChecksCompleteTokens() {
        JsonExtractionPlan plan = JsonRecordExtractor.compile("groups[0].items[*]", Collections.singletonList(
                new JsonFieldProjection("id", null, "groups[0].items[*]")));
        assertEquals(2, plan.extract(JSON.parseObject(
                "{\"groups\":[{\"items\":[{\"id\":1},{\"id\":2}]}]}"), 0).getSourceRowCount());
        assertCompileFails("data1[*]", Collections.singletonList(
                new JsonFieldProjection("id", null, "data10[*]")), "not a prefix");
        assertCompileFails("data[*].items[*]", Collections.singletonList(
                new JsonFieldProjection("id", null, "data[0].items[*]")), "not a prefix");
    }

    @Test
    public void planCopiesProjectionListAndKeepsStateIndependentAcrossExtractions() {
        List<JsonFieldProjection> fields = new ArrayList<>();
        fields.add(new JsonFieldProjection("id", null, "data[*]"));
        JsonExtractionPlan plan = JsonRecordExtractor.compile("data[*]", fields);
        fields.clear();

        JsonExtractionResult limited = plan.extract(JSON.parseObject("{\"data\":[{\"id\":1},{\"id\":2}]}"), 1);
        JsonExtractionResult complete = plan.extract(JSON.parseObject("{\"data\":[{\"id\":3}]}"), 0);

        assertEquals(2, limited.getSourceRowCount());
        assertEquals(1, complete.getSourceRowCount());
        assertEquals(3, complete.getRows().getJSONObject(0).getIntValue("id"));
    }

    @Test
    public void outputLimitShortCircuitsBeforeUnvisitedMalformedRows() {
        JsonExtractionPlan plan = JsonRecordExtractor.compile("data[*]", Collections.singletonList(
                new JsonFieldProjection("id", null, "data[*]")));
        assertEquals(1, plan.extract(JSON.parseObject("{\"data\":[{\"id\":1},\"not-an-object\"]}"), 1)
                .getRows().size());
        try {
            plan.extract(JSON.parseObject("{\"data\":[{\"id\":1},\"not-an-object\"]}"), 0);
            fail("expected a row type error when all rows are visited");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("expected object"));
        }
    }

    private void assertCompileFails(String recordPath, java.util.List<JsonFieldProjection> fields, String message) {
        try {
            JsonRecordExtractor.compile(recordPath, fields);
            fail("expected compile error containing " + message);
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains(message));
        }
    }
}
