package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.transfer.model.TransferEndpoint;
import com.jdragon.aggregation.transfer.model.TransferMapping;
import com.jdragon.aggregation.transfer.model.TransferPolicy;
import com.jdragon.aggregation.transfer.model.TransferSelection;
import com.jdragon.aggregation.transfer.model.TransferSpec;
import com.jdragon.aggregation.transfer.model.TransferRuntimeOptions;
import org.junit.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class DynamicTemplateResolverTest {

    private static final Instant PLANNED_AT = Instant.parse("2026-08-28T05:14:15Z");

    @Test
    public void resolvesFunctionsBeforeRunTemplatesAndKeepsPerFilePlaceholders() {
        TransferSpec spec = spec(
                "/incoming/$getCurrentTime('yyyyMMdd', '-1d')",
                List.of("**/*_$getCurrentTime('yyyyMMdd', '-1d').txt"),
                "/archive/$getCurrentTime('yyyyMMdd')/${runId}",
                "$getCurrentTime('yyyyMM')/${relativePath}");
        TransferSpec resolved = new DynamicTemplateResolver().resolve(spec, "run-1", PLANNED_AT);
        assertEquals("/incoming/20260827", resolved.selection().rootPath());
        assertEquals("**/*_20260827.txt", resolved.selection().includeGlobs().get(0));
        assertEquals("/archive/20260828/run-1", resolved.mapping().targetRootPath());
        assertEquals("202608/${relativePath}", resolved.mapping().targetPathTemplate());
    }

    @Test
    public void rejectsPerFileTemplatesOutsideTargetTemplate() {
        TransferSpec spec = spec("/incoming", List.of("${fileName}"), "/archive", null);
        assertThrows(IllegalArgumentException.class,
                () -> new DynamicTemplateResolver().resolve(spec, "run-1", PLANNED_AT));
    }

    @Test
    public void resolvesEverySelectionExpressionAsOneLineAndCompilesDynamicRegexLater() {
        TransferSpec spec = spec(
                "/incoming/$getCurrentTime('yyyyMMdd')",
                List.of("$subStr('a,b', 0, 3)"),
                "/archive/$getCurrentTime('yyyyMMdd')",
                null);
        TransferSpec resolved = new DynamicTemplateResolver().resolve(spec, "run-1", PLANNED_AT);
        assertEquals("a,b", resolved.selection().includeGlobs().get(0));
    }

    @Test
    public void rejectsMissingParameterAndInvalidTimeTemplate() {
        TransferSpec missing = spec("/incoming/${param:day}", List.of(), "/archive", null);
        assertThrows(IllegalArgumentException.class,
                () -> new DynamicTemplateResolver().resolve(missing, "run-1", PLANNED_AT));
        TransferSpec invalid = spec("/incoming/${now:bad[}", List.of(), "/archive", null);
        assertThrows(IllegalArgumentException.class,
                () -> new DynamicTemplateResolver().resolve(invalid, "run-1", PLANNED_AT));
    }

    @Test
    public void rejectsNestedTemplateFormsAndUnclosedExpressions() {
        TransferSpec nested = spec("/incoming/${param:$getCurrentTime('yyyyMMdd')}",
                List.of(), "/archive", null);
        assertThrows(IllegalArgumentException.class,
                () -> new DynamicTemplateResolver().resolve(nested, "run-1", PLANNED_AT));
        TransferSpec unclosed = spec("/incoming/${runId", List.of(), "/archive", null);
        assertThrows(IllegalArgumentException.class,
                () -> new DynamicTemplateResolver().resolve(unclosed, "run-1", PLANNED_AT));
        TransferSpec empty = spec("/incoming/${}", List.of(), "/archive", null);
        assertThrows(IllegalArgumentException.class,
                () -> new DynamicTemplateResolver().resolve(empty, "run-1", PLANNED_AT));
    }

    @Test
    public void keepsPerFilePlaceholdersUntilPlannerMapsEachFile() {
        TransferSpec spec = spec("/incoming", List.of(), "/archive", "${relativePath}/${fileName}");
        TransferSpec resolved = new DynamicTemplateResolver().resolve(spec, "run-1", PLANNED_AT);
        assertEquals("${relativePath}/${fileName}", resolved.mapping().targetPathTemplate());
    }

    @Test
    public void preservesExplicitBlankResolvedPathsForPlannerValidation() {
        TransferSpec blankSource = spec("$subStr('', 0, 0)", List.of(), "/archive", null);
        TransferSpec resolvedSource = new DynamicTemplateResolver().resolve(blankSource, "run-1", PLANNED_AT);
        assertEquals("", resolvedSource.selection().rootPath());
        TransferSpec blankTarget = spec("/incoming", List.of(), "$subStr('', 0, 0)", null);
        TransferSpec resolvedTarget = new DynamicTemplateResolver().resolve(blankTarget, "run-1", PLANNED_AT);
        assertEquals("", resolvedTarget.mapping().targetRootPath());
    }

    private TransferSpec spec(String root, List<String> globs, String targetRoot, String targetTemplate) {
        return new TransferSpec(1,
                new TransferEndpoint("local", "source", Map.of()),
                new TransferEndpoint("local", "target", Map.of()),
                new TransferSelection(root, List.of(), true, globs, null, List.of(), null, null,
                        null, null, 100),
                new TransferMapping(targetRoot, true, targetTemplate),
                TransferPolicy.defaults(), TransferRuntimeOptions.defaults(), "Asia/Shanghai", Map.of());
    }
}
