package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.transfer.model.SourceSnapshot;
import com.jdragon.aggregation.transfer.model.TransferEvent;
import com.jdragon.aggregation.transfer.model.TransferEventType;
import com.jdragon.aggregation.transfer.model.TransferMapping;
import com.jdragon.aggregation.transfer.model.TransferPlan;
import com.jdragon.aggregation.transfer.model.TransferPlanItem;
import com.jdragon.aggregation.transfer.model.TransferSpec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class TransferPlanner {

    private final FileDiscovery discovery;
    private final DynamicTemplateResolver templateResolver;

    public TransferPlanner() {
        this(new FileDiscovery(), new DynamicTemplateResolver());
    }

    public TransferPlanner(FileDiscovery discovery, DynamicTemplateResolver templateResolver) {
        this.discovery = discovery;
        this.templateResolver = templateResolver;
    }

    public PreparedTransfer prepare(TransferSpec spec, TransferFileSystem source, String runId,
                                    Instant plannedAt, TransferEventListener listener) throws IOException {
        TransferEventListener events = listener == null ? TransferEventListener.NOOP : listener;
        Instant effectivePlannedAt = plannedAt == null ? Instant.now() : plannedAt;
        TransferSpec resolved = templateResolver.resolve(spec, runId, effectivePlannedAt);
        events.onEvent(event(TransferEventType.PLAN_STARTED, runId, null, 0, 0,
                "file discovery started", Map.of()));

        List<TransferPlanItem> items = new ArrayList<>();
        Set<String> targetPaths = new HashSet<>();
        discovery.discover(source, resolved.selection(), file -> {
            TransferPlanItem item = toPlanItem(resolved, runId, file);
            String targetKey = TransferPaths.normalize(item.targetPath()).toLowerCase();
            if (!targetPaths.add(targetKey)) {
                throw new IllegalArgumentException("multiple source files map to target path: " + item.targetPath());
            }
            items.add(item);
            events.onEvent(event(TransferEventType.ITEM_PLANNED, runId, item.itemId(),
                    0, item.sourceSnapshot().size(), "file added to transfer plan",
                    Map.of("sourcePath", item.sourcePath(), "targetPath", item.targetPath())));
        });
        events.onEvent(event(TransferEventType.PLAN_COMPLETED, runId, null, 0,
                items.stream().mapToLong(item -> item.sourceSnapshot().size()).sum(),
                "file discovery completed", Map.of("fileCount", items.size())));
        return new PreparedTransfer(resolved, new TransferPlan(runId, effectivePlannedAt, items));
    }

    private TransferPlanItem toPlanItem(TransferSpec spec, String runId, DiscoveredFile file) {
        TransferMapping mapping = spec.mapping();
        String mapped = mapping.preserveRelativePath()
                ? file.relativePath()
                : file.entry().name();
        if (mapping.targetPathTemplate() != null && !mapping.targetPathTemplate().isBlank()) {
            mapped = mapping.targetPathTemplate()
                    .replace("${relativePath}", file.relativePath())
                    .replace("${fileName}", file.entry().name());
        }
        String targetPath = TransferPaths.isAbsolute(mapped)
                ? TransferPaths.normalize(mapped)
                : TransferPaths.join(mapping.targetRootPath(), mapped);
        String itemSeed = runId + '\0' + file.entry().path() + '\0' + targetPath;
        String itemId = UUID.nameUUIDFromBytes(itemSeed.getBytes(StandardCharsets.UTF_8)).toString();
        String temporaryPath = targetPath + ".filetransfer." + runId + "." + itemId + ".part";
        return new TransferPlanItem(itemId, spec.source().identity(), spec.target().identity(),
                file.entry().path(), targetPath, temporaryPath, file.relativePath(),
                SourceSnapshot.from(file.entry()));
    }

    private TransferEvent event(TransferEventType type, String runId, String itemId,
                                long transferred, long total, String message, Map<String, Object> details) {
        return new TransferEvent(type, Instant.now(), runId, itemId, null,
                transferred, total, 0, message, null, details);
    }
}
