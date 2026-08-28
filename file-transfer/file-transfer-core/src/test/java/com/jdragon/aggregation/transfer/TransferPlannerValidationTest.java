package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.transfer.model.TransferEndpoint;
import com.jdragon.aggregation.transfer.model.TransferMapping;
import com.jdragon.aggregation.transfer.model.TransferSelection;
import com.jdragon.aggregation.transfer.model.TransferSpec;
import org.junit.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertThrows;

public class TransferPlannerValidationTest {

    @Test
    public void rejectsBlankResolvedSourceBeforeDiscovery() {
        FileDiscovery discovery = new FileDiscovery() {
            @Override
            public int discover(com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem source,
                                TransferSelection selection,
                                java.util.function.Consumer<DiscoveredFile> consumer) {
                throw new AssertionError("discovery should not be reached");
            }
        };
        TransferPlanner planner = new TransferPlanner(discovery, new DynamicTemplateResolver());
        TransferSpec spec = spec("$subStr('', 0, 0)", "/archive", null);
        assertThrows(IllegalArgumentException.class,
                () -> planner.prepare(spec, null, null, "run", Instant.EPOCH, null));
    }

    @Test
    public void validatesResolvedPathsAndRegexBeforeOpeningFiles() {
        TransferSpec blankTarget = spec("/incoming", "$subStr('', 0, 0)", null);
        assertThrows(IllegalArgumentException.class,
                () -> new TransferPlanner().prepare(blankTarget, null, null, "run", Instant.EPOCH, null));

        TransferSpec invalidRegex = spec("/incoming", "/archive", "[broken");
        assertThrows(IllegalArgumentException.class,
                () -> new TransferPlanner().prepare(invalidRegex, null, null, "run", Instant.EPOCH, null));
    }

    private TransferSpec spec(String root, String target, String regex) {
        return new TransferSpec(1,
                new TransferEndpoint("local", "source", Map.of()),
                new TransferEndpoint("local", "target", Map.of()),
                new TransferSelection(root, List.of(), true, List.of(), regex,
                        List.of(), null, null, null, null, 100),
                new TransferMapping(target, true, null), null, null, "UTC", Map.of());
    }
}
