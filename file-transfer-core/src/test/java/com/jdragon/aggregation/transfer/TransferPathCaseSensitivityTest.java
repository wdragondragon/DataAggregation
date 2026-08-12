package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.datasource.file.transfer.StorageCapabilities;
import com.jdragon.aggregation.transfer.model.TransferEndpoint;
import com.jdragon.aggregation.transfer.model.TransferMapping;
import com.jdragon.aggregation.transfer.model.TransferSelection;
import com.jdragon.aggregation.transfer.model.TransferSpec;
import org.junit.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class TransferPathCaseSensitivityTest {

    @Test
    public void relativePathComparisonPreservesCase() {
        assertEquals("A.txt", TransferPaths.relativeTo("/source", "/source/A.txt"));
        assertEquals("A.txt", TransferPaths.relativeTo("/SOURCE", "/source/A.txt", false));
        assertThrows(IllegalArgumentException.class,
                () -> TransferPaths.relativeTo("/source", "/SOURCE/A.txt"));
    }

    @Test
    public void plannerAllowsTargetsThatDifferOnlyByCase() throws Exception {
        FileDiscovery discovery = new FileDiscovery() {
            @Override
            public int discover(com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem source,
                                TransferSelection selection,
                                java.util.function.Consumer<DiscoveredFile> consumer) {
                consumer.accept(file("A.txt"));
                consumer.accept(file("a.txt"));
                return 2;
            }
        };
        TransferSpec spec = new TransferSpec(1,
                new TransferEndpoint("source", "source", Map.of()),
                new TransferEndpoint("target", "target", Map.of()),
                new TransferSelection("/source", List.of(), true, List.of(), null,
                        List.of(), null, null, null, null, 100),
                new TransferMapping("/target", true, null), null, null, "UTC", Map.of());

        PreparedTransfer prepared = new TransferPlanner(discovery, new DynamicTemplateResolver())
                .prepare(spec, null, target(true), "run-1", Instant.EPOCH, null);

        assertEquals(List.of("/target/A.txt", "/target/a.txt"), prepared.plan().items().stream()
                .map(item -> item.targetPath()).toList());
    }

    @Test
    public void plannerRejectsCaseOnlyCollisionForCaseInsensitiveTarget() {
        TransferPlanner planner = new TransferPlanner(discovery("A.txt", "a.txt"),
                new DynamicTemplateResolver());

        assertThrows(IllegalArgumentException.class,
                () -> planner.prepare(spec(), null, target(false), "run-2", Instant.EPOCH, null));
    }

    @Test
    public void plannerRejectsExactDuplicateForCaseSensitiveTarget() {
        TransferPlanner planner = new TransferPlanner(discovery("same.txt", "same.txt"),
                new DynamicTemplateResolver());

        assertThrows(IllegalArgumentException.class,
                () -> planner.prepare(spec(), null, target(true), "run-3", Instant.EPOCH, null));
    }

    private FileDiscovery discovery(String... names) {
        return new FileDiscovery() {
            @Override
            public int discover(com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem source,
                                TransferSelection selection,
                                java.util.function.Consumer<DiscoveredFile> consumer) {
                for (String name : names) {
                    consumer.accept(file(name));
                }
                return names.length;
            }
        };
    }

    private TransferSpec spec() {
        return new TransferSpec(1,
                new TransferEndpoint("source", "source", Map.of()),
                new TransferEndpoint("target", "target", Map.of()),
                new TransferSelection("/source", List.of(), true, List.of(), null,
                        List.of(), null, null, null, null, 100),
                new TransferMapping("/target", true, null), null, null, "UTC", Map.of());
    }

    private TestTransferFileSystem target(boolean caseSensitive) {
        return new TestTransferFileSystem() {
            @Override
            public StorageCapabilities capabilities() {
                return new StorageCapabilities(false, false, false, false, false, false,
                        Set.of(), caseSensitive);
            }
        };
    }

    private DiscoveredFile file(String name) {
        return new DiscoveredFile(name,
                new TransferFileEntry("/source/" + name, name, false, 1L, 0L, null));
    }
}
