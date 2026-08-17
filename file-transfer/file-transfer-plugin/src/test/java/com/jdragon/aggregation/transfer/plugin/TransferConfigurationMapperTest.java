package com.jdragon.aggregation.transfer.plugin;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.transfer.model.ConflictPolicy;
import com.jdragon.aggregation.transfer.model.SourceSuccessAction;
import com.jdragon.aggregation.transfer.model.TransferSpec;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TransferConfigurationMapperTest {

    private final TransferConfigurationMapper mapper = new TransferConfigurationMapper();

    @Test
    public void mapsDefaultsAndEndpointConfiguration() {
        TransferSpec spec = mapper.map(configuration(""));

        assertEquals(1, spec.schemaVersion());
        assertEquals("local", spec.source().plugin());
        assertEquals("source-1", spec.source().identity());
        assertEquals("C:/source", spec.source().config().get("rootPath"));
        assertTrue(spec.selection().recursive());
        assertEquals(100_000, spec.selection().maxFiles());
        assertEquals(ConflictPolicy.FAIL, spec.policy().conflictPolicy());
        assertEquals(SourceSuccessAction.KEEP, spec.policy().sourceSuccessAction());
        assertEquals(4, spec.runtime().concurrency());
        assertEquals("20260807", spec.parameters().get("day"));
    }

    @Test
    public void mapsRuntimeAndPolicyOverrides() {
        TransferSpec spec = mapper.map(configuration(",\n"
                + "  \"policy\": {\"conflictPolicy\": \"backup-then-overwrite\","
                + " \"sourceSuccessAction\": \"delete\"},\n"
                + "  \"runtime\": {\"concurrency\": 8, \"maxRetries\": 0,"
                + " \"chunkSizeBytes\": 65536}"));

        assertEquals(ConflictPolicy.BACKUP_THEN_OVERWRITE, spec.policy().conflictPolicy());
        assertEquals(SourceSuccessAction.DELETE, spec.policy().sourceSuccessAction());
        assertEquals(8, spec.runtime().concurrency());
        assertEquals(0, spec.runtime().maxRetries());
        assertEquals(65_536, spec.runtime().chunkSizeBytes());
    }

    @Test
    public void rejectsUnsupportedSchema() {
        Configuration configuration = configuration("");
        configuration.set("schemaVersion", 2);

        assertThrows(IllegalArgumentException.class, () -> mapper.map(configuration));
    }

    private Configuration configuration(String extra) {
        return Configuration.from("{\n"
                + "  \"schemaVersion\": 1,\n"
                + "  \"source\": {\"plugin\": \"local\", \"identity\": \"source-1\","
                + " \"config\": {\"rootPath\": \"C:/source\"}},\n"
                + "  \"target\": {\"plugin\": \"local\","
                + " \"config\": {\"rootPath\": \"C:/target\"}},\n"
                + "  \"selection\": {\"rootPath\": \"C:/source\"},\n"
                + "  \"mapping\": {\"targetRootPath\": \"C:/target\"},\n"
                + "  \"parameters\": {\"day\": 20260807}"
                + extra + "\n}");
    }
}
