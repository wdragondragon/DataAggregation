package com.jdragon.aggregation.plugin.httpdyn.impl;

import org.junit.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class GetCurrentTimeTest {

    private static final Clock FIXED_CLOCK = Clock.fixed(
            Instant.parse("2024-02-29T23:45:30Z"), ZoneId.of("UTC"));

    @Test
    public void shouldFormatCurrentTimeWithoutOffset() {
        GetCurrentTime executor = new GetCurrentTime(FIXED_CLOCK);

        assertEquals("20240229-234530", executor.execute("yyyyMMdd-HHmmss"));
    }

    @Test
    public void shouldAcceptQuotedArgumentsAndCompoundOffset() {
        GetCurrentTime executor = new GetCurrentTime(FIXED_CLOCK);

        assertEquals("20240229 004530",
                executor.execute("\"yyyyMMdd HHmmss\"", "'-1d+1h'"));
    }

    @Test
    public void shouldApplyAllSupportedUnits() {
        GetCurrentTime executor = new GetCurrentTime(FIXED_CLOCK);

        assertEquals("2023", executor.execute("yyyy", "-1y"));
        assertEquals("01", executor.execute("MM", "-1m"));
        assertEquals("22", executor.execute("dd", "-1w"));
        assertEquals("28", executor.execute("dd", "-1d"));
        assertEquals("22", executor.execute("HH", "-1h"));
        assertEquals("44", executor.execute("mm", "-1mi"));
        assertEquals("29", executor.execute("ss", "-1s"));
    }

    @Test
    public void shouldTreatBareNumberAsSecondsAndSupportMultiplication() {
        GetCurrentTime executor = new GetCurrentTime(FIXED_CLOCK);

        assertEquals("23:46:30", executor.execute("HH:mm:ss", "60"));
        assertEquals("23:46:30", executor.execute("HH:mm:ss", "2*30"));
        assertEquals("23:44:30", executor.execute("HH:mm:ss", "-2*30"));
    }

    @Test
    public void shouldRejectMissingOrInvalidArguments() {
        GetCurrentTime executor = new GetCurrentTime(FIXED_CLOCK);

        assertThrows(IllegalArgumentException.class, () -> executor.execute());
        assertThrows(IllegalArgumentException.class, () -> executor.execute(""));
        assertThrows(IllegalArgumentException.class, () -> executor.execute("yyyy", "1x"));
        assertThrows(IllegalArgumentException.class, () -> executor.execute("yyyy", "1d+oops"));
        assertThrows(IllegalArgumentException.class, () -> executor.execute("invalid pattern"));
    }
}
