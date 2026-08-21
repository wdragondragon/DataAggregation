package com.jdragon.aggregation.datasource.file.utils;

import com.jdragon.aggregation.datasource.file.utils.efile.EFile;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class EFileUtilTest {

    @Test
    public void shouldPreserveEmptyDataColumns() throws Exception {
        String content = "<! Entity='demo' dataTime='20260820_00:00:00' type='test' !>\n"
                + "<T01::Demo planDate='20260820' >\n"
                + "@ first second third\n"
                + "// first second third\n"
                + "# 1  123\n"
                + "#  2 456\n"
                + "# 3 789 \n"
                + "</T01::Demo>\n";

        EFile eFile = new EFileUtil().parseEFile(
                new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                "efile",
                Collections.emptyList(),
                "UTF-8");

        List<List<String>> rows = eFile.getEFileDetails().get(0).getData();
        assertEquals(3, rows.size());
        assertEquals(List.of("1", "", "123"), rows.get(0));
        assertEquals(List.of("", "2", "456"), rows.get(1));
        assertEquals(List.of("3", "789", ""), rows.get(2));
    }

    @Test
    public void shouldSplitQuotedValuesWithoutCollapsingEmptyColumns() {
        EFileUtil util = new EFileUtil();

        assertArrayEquals(new String[]{"A B", "", "1"}, util.splitData("'A B'  1"));
        assertArrayEquals(new String[]{"A B", "1"}, util.splitData("\"A B\" 1"));
    }

    @Test
    public void shouldParseTableCodeAndTableNameInDocumentOrder() throws Exception {
        EFile eFile = parse("<AGG_TAG::agg_tag planDate='20260820' >");

        assertEquals("AGG_TAG", eFile.getEFileDetails().get(0).getEFileTableInfo().getTableCode());
        assertEquals("agg_tag", eFile.getEFileDetails().get(0).getEFileTableInfo().getTableName());
        assertEquals("20260820", eFile.getEFileDetails().get(0).getEFileTableInfo().getPlanDate());
    }

    @Test
    public void shouldGenerateTagHeaderWithOptionalPlanDate() {
        assertEquals("<AGG_TAG::agg_tag planDate='20260820' >",
                EFileUtil.genTagHeader("AGG_TAG", "agg_tag", " 20260820 "));
        assertEquals("<AGG_TAG::agg_tag >", EFileUtil.genTagHeader("AGG_TAG", "agg_tag", null));
        assertEquals("<AGG_TAG::agg_tag >", EFileUtil.genTagHeader("AGG_TAG", "agg_tag", ""));
        assertEquals("<AGG_TAG::agg_tag >", EFileUtil.genTagHeader("AGG_TAG", "agg_tag", "   "));
    }

    @Test
    public void shouldParseBlockWithoutPlanDate() throws Exception {
        EFile eFile = parse("<AGG_TAG::agg_tag tag_code='A' >");

        assertEquals("AGG_TAG", eFile.getEFileDetails().get(0).getEFileTableInfo().getTableCode());
        assertEquals("agg_tag", eFile.getEFileDetails().get(0).getEFileTableInfo().getTableName());
        assertNull(eFile.getEFileDetails().get(0).getEFileTableInfo().getPlanDate());
    }

    private EFile parse(String tagHeader) throws Exception {
        String content = "<! Entity='demo' dataTime='20260820_00:00:00' type='test' !>\n"
                + tagHeader + "\n"
                + "@ id\n"
                + "// id\n"
                + "# 1\n"
                + "</AGG_TAG::agg_tag>\n";
        return new EFileUtil().parseEFile(
                new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)),
                "efile",
                Collections.emptyList(),
                "UTF-8");
    }
}
