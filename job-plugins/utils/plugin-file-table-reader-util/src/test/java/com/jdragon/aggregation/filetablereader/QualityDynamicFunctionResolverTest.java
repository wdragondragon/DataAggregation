package com.jdragon.aggregation.filetablereader;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QualityDynamicFunctionResolverTest {

    @Test
    public void shouldReplaceQualityTimeFunction() {
        String value = QualityDynamicFunctionResolver.replaceAll("/data/$getCurrentTime(yyyyMMdd,0)/part.csv");

        assertTrue(value.matches("/data/\\d{8}/part\\.csv"));
    }

    @Test
    public void shouldAllowCurrentTimeOffsetToBeOmittedLikeStudioQualityFunction() {
        String value = QualityDynamicFunctionResolver.replaceAll("/data/$getCurrentTime(yyyyMMddHHmmss)/part.csv");

        assertTrue(value.matches("/data/\\d{14}/part\\.csv"));
    }

    @Test
    public void shouldSupportQualityStringFunctions() {
        assertEquals("prefix-LLOW-ell-abc-ABC", QualityDynamicFunctionResolver.replaceAll(
                "prefix-$subStr(\"HELLOWORLD\",2,4)-$subString(\"HelloWorld\",1,4)-$toLower(\"ABC\")-$toUpper(\"abc\")"));
    }

    @Test
    public void shouldReturnBlankForUnsupportedFunctionLikeQualityFunctionExecuter() {
        assertEquals("/data//part.csv", QualityDynamicFunctionResolver.replaceAll("/data/$notExists(abc)/part.csv"));
    }

    @Test
    public void shouldSupportOffsetUnitExpression() {
        String value = QualityDynamicFunctionResolver.replaceAll("$getCurrentTime(yyyyMMddHH, -1d+1h)");

        assertTrue(value.matches("\\d{10}"));
    }

    @Test
    public void shouldSupportMonthLastDayAndTimeUnitValue() {
        assertEquals("31", QualityDynamicFunctionResolver.replaceAll("$getTheMonthLastDay(1558508400000)"));
        assertEquals("2024", QualityDynamicFunctionResolver.replaceAll("$getTimeUnitValue(20240522175615,\"y\")"));
    }
}
