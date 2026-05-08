package com.jdragon.aggregation.core.streaming;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PartitionedSpillStoreTest {

    @Test
    public void shouldResolvePartitionForIntegerMinHash() throws Exception {
        String minHashKey = new String(new char[]{2, 13, 0, 9, 30, 12, 2});
        assertEquals(Integer.MIN_VALUE, minHashKey.hashCode());

        Path spillBase = Files.createTempDirectory("partitioned-spill-test");
        PartitionedSpillStore store = new PartitionedSpillStore("min-hash", spillBase.toString(), 7, false);
        try {
            int partition = store.resolvePartition(minHashKey);
            assertTrue(partition >= 0);
            assertTrue(partition < store.getPartitionCount());
        } finally {
            store.cleanup();
            Files.deleteIfExists(spillBase);
        }
    }
}
