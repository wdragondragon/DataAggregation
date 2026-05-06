package com.jdragon.aggregation.filetablereader;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FilePartitionMatcherTest {

    @Test
    public void shouldMatchSingleSegmentGlobWithoutCrossingDirectories() {
        FilePartitionMatcher matcher = FilePartitionMatcher.compile("glob", "dt=*/part-?.{csv,e}");

        assertTrue(matcher.matches("dt=20260505/part-1.csv"));
        assertTrue(matcher.matches("dt=20260505/part-2.e"));
        assertFalse(matcher.matches("dt=20260505/part-10.csv"));
        assertFalse(matcher.matches("dt=20260505/nested/part-1.csv"));
    }

    @Test
    public void shouldMatchDoubleStarGlobAcrossDirectories() {
        FilePartitionMatcher matcher = FilePartitionMatcher.compile("glob", "**/*.csv");

        assertTrue(matcher.matches("dt=20260505/part-1.csv"));
        assertTrue(matcher.matches("dt=20260505/hour=10/part-1.csv"));
        assertFalse(matcher.matches("dt=20260505/hour=10/part-1.txt"));
    }

    @Test
    public void shouldNormalizeBackslashAndLeadingSlashForCandidatePath() {
        FilePartitionMatcher matcher = FilePartitionMatcher.compile("glob", "dt=*/part-*.csv");

        assertTrue(matcher.matches("/dt=20260505/part-1.csv"));
        assertTrue(matcher.matches("\\dt=20260505\\part-1.csv"));
    }

    @Test
    public void shouldUseRegexPartitionDirectly() {
        FilePartitionMatcher matcher = FilePartitionMatcher.compile("regex", "dt=.*/part-[0-9]+\\.csv");

        assertTrue(matcher.matches("dt=20260505/part-12.csv"));
        assertFalse(matcher.matches("dt=20260505/part-a.csv"));
        assertFalse(matcher.matches("dt=20260505/part-12Xcsv"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectUnsupportedPartitionType() {
        FilePartitionMatcher.compile("ant", "**/*.csv");
    }
}
