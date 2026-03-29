package com.jdragon.aggregation.core.streaming;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SpillGuardTest {

    @Test
    public void throwsWhenTotalSpillBytesExceeded() throws Exception {
        Path spillRoot = Files.createTempDirectory("spill-guard-limit");
        SpillGuard guard = new SpillGuard(128L, 1L);
        PartitionedSpillStore store = new PartitionedSpillStore("spill-guard-limit", spillRoot.toString(), 1, false, guard);

        try {
            store.append("s1", CompositeKey.fromRecord(row(1, 512), Collections.singletonList("id")), row(1, 512));
            fail("Expected spill limit to be enforced");
        } catch (SpillLimitExceededException expected) {
            assertTrue(expected.getMessage().contains("Spill limit exceeded"));
        } finally {
            store.cleanup();
        }
    }

    @Test
    public void throwsWhenFreeDiskWatermarkWouldBeViolated() throws Exception {
        Path spillRoot = Files.createTempDirectory("spill-guard-disk");
        SpillGuard guard = new SpillGuard(Long.MAX_VALUE / 4L, Long.MAX_VALUE / 8L);
        PartitionedSpillStore store = new PartitionedSpillStore("spill-guard-disk", spillRoot.toString(), 1, false, guard);

        try {
            store.append("s1", CompositeKey.fromRecord(row(1, 8), Collections.singletonList("id")), row(1, 8));
            fail("Expected free disk watermark to be enforced");
        } catch (SpillLimitExceededException expected) {
            assertTrue(expected.getMessage().contains("Insufficient free disk"));
        } finally {
            store.cleanup();
        }
    }

    @Test
    public void sharesQuotaAcrossMultipleSpillStores() throws Exception {
        Path spillRoot = Files.createTempDirectory("spill-guard-shared");
        SpillGuard guard = new SpillGuard(380L, 1L);
        PartitionedSpillStore first = new PartitionedSpillStore("spill-guard-a", spillRoot.toString(), 1, false, guard);
        PartitionedSpillStore second = new PartitionedSpillStore("spill-guard-b", spillRoot.toString(), 1, false, guard);

        try {
            first.append("s1", CompositeKey.fromRecord(row(1, 64), Collections.singletonList("id")), row(1, 64));
            second.append("s2", CompositeKey.fromRecord(row(2, 64), Collections.singletonList("id")), row(2, 64));
            assertTrue(guard.getTotalReservedBytes() > 0L);

            try {
                second.append("s2", CompositeKey.fromRecord(row(3, 256), Collections.singletonList("id")), row(3, 256));
                fail("Expected shared spill guard to reject cumulative spill usage");
            } catch (SpillLimitExceededException expected) {
                assertTrue(expected.getMessage().contains("Spill limit exceeded"));
            }
        } finally {
            first.cleanup();
            second.cleanup();
        }
    }

    private Map<String, Object> row(int id, int payloadSize) {
        Map<String, Object> row = new LinkedHashMap<String, Object>();
        row.put("id", id);
        row.put("payload", repeat('x', payloadSize));
        return row;
    }

    private String repeat(char ch, int count) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            builder.append(ch);
        }
        return builder.toString();
    }
}
