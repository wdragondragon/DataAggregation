package com.jdragon.aggregation.commons.util;

import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DynamicFunctionResolverTest {

    private static final LocalDateTime BASE = LocalDateTime.of(2026, 8, 28, 13, 14, 15);

    @Test
    public void resolvesSupportedFunctionsAgainstProvidedBaseTime() {
        assertEquals("20260827", DynamicFunctionResolver.replaceAll(
                "$getCurrentTime('yyyyMMdd', '-1d')", BASE, DynamicFunctionResolver.Mode.STRICT));
        assertEquals("31", DynamicFunctionResolver.replaceAll(
                "$getTheMonthLastDay('2026-08-28 00:00:00')", BASE, DynamicFunctionResolver.Mode.STRICT));
        assertEquals("2026", DynamicFunctionResolver.replaceAll(
                "$getTimeUnitValue('20260828131415', 'y')", BASE, DynamicFunctionResolver.Mode.STRICT));
        assertEquals("ABC", DynamicFunctionResolver.replaceAll(
                "$subStr('ABC123', 0, 3)", BASE, DynamicFunctionResolver.Mode.STRICT));
        assertEquals("BC", DynamicFunctionResolver.replaceAll(
                "$subString('ABC123', 1, 3)", BASE, DynamicFunctionResolver.Mode.STRICT));
        assertEquals("abc", DynamicFunctionResolver.replaceAll(
                "$toLower('ABC')", BASE, DynamicFunctionResolver.Mode.STRICT));
        assertEquals("ABC", DynamicFunctionResolver.replaceAll(
                "$toUpper('abc')", BASE, DynamicFunctionResolver.Mode.STRICT));
    }

    @Test
    public void strictModeRejectsUnknownAndMalformedExpressions() {
        assertFalse(DynamicFunctionResolver.validate("$unknown('x')", DynamicFunctionResolver.Mode.STRICT).isEmpty());
        assertFalse(DynamicFunctionResolver.validate("$getCurrentTime('yyyyMMdd'", DynamicFunctionResolver.Mode.STRICT).isEmpty());
        assertFalse(DynamicFunctionResolver.validate("$getCurrentTime('yyyyMMdd', '-xd')", DynamicFunctionResolver.Mode.STRICT).isEmpty());
        assertTrue(DynamicFunctionResolver.validate("/path/$getCurrentTime('yyyyMMdd')", DynamicFunctionResolver.Mode.STRICT).isEmpty());
    }

    @Test
    public void compatibleModeKeepsUnknownFunctionFallback() {
        assertEquals("/data/", DynamicFunctionResolver.replaceAll(
                "/data/$notExists('x')", BASE, DynamicFunctionResolver.Mode.COMPATIBLE));
    }

    @Test
    public void compatibleModeKeepsHistoricalLeniencyForKnownFunctions() {
        assertEquals("20260828", DynamicFunctionResolver.replaceAll(
                "$getCurrentTime('yyyyMMdd', 0, ignored)", BASE,
                DynamicFunctionResolver.Mode.COMPATIBLE));
        assertEquals("0", DynamicFunctionResolver.replaceAll(
                "$getTimeUnitValue(20260828, unsupported)", BASE,
                DynamicFunctionResolver.Mode.COMPATIBLE));
        assertEquals("'ABC", DynamicFunctionResolver.replaceAll(
                "$toUpper('abc)", BASE, DynamicFunctionResolver.Mode.COMPATIBLE));
    }

    @Test
    public void resolvesEpochArgumentsUsingProvidedZone() {
        long epoch = java.time.Instant.parse("2026-08-28T23:30:00Z").toEpochMilli();
        assertEquals("29", DynamicFunctionResolver.replaceAll(
                "$getTimeUnitValue('" + epoch + "', 'd')", BASE,
                DynamicFunctionResolver.Mode.STRICT, ZoneId.of("Asia/Shanghai")));
    }

    @Test
    public void rejectsNestedTemplateAndUnterminatedQuotes() {
        assertFalse(DynamicFunctionResolver.validate(
                "$toLower(${fileName})", DynamicFunctionResolver.Mode.STRICT).isEmpty());
        assertFalse(DynamicFunctionResolver.validate(
                "$toUpper('abc)", DynamicFunctionResolver.Mode.STRICT).isEmpty());
    }
}
