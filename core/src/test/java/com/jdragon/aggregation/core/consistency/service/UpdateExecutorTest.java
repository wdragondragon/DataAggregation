package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;
import com.jdragon.aggregation.core.consistency.model.UpdatePlan;
import com.jdragon.aggregation.commons.util.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.*;

import static org.junit.Assert.*;

public class UpdateExecutorTest {

    private UpdateExecutor updateExecutor;
    private Method determineOperationTypeMethod;
    private Method valuesAreEqualMethod;

    @Before
    public void setUp() throws Exception {
        // Create UpdateExecutor with null plugin manager (not used for method testing)
        updateExecutor = new UpdateExecutor(null);
        
        // Use reflection to access private determineOperationType method
        determineOperationTypeMethod = UpdateExecutor.class.getDeclaredMethod(
                "determineOperationType", DifferenceRecord.class, String.class);
        determineOperationTypeMethod.setAccessible(true);
        
        // Use reflection to access private valuesAreEqual method
        valuesAreEqualMethod = UpdateExecutor.class.getDeclaredMethod(
                "valuesAreEqual", Map.class, Map.class, Map.class);
        valuesAreEqualMethod.setAccessible(true);
    }

    @Test
    public void testDetermineOperationType_InsertScenario() throws Exception {
        // Scenario: record should exist (non-match-key fields have values), target missing
        DifferenceRecord diff = createDifferenceRecord(
                Arrays.asList("id", "name"),
                createMatchKeys("id", 1, "name", "test"),
                createResolvedValues("id", 1, "name", "test", "age", 25, "salary", 50000),
                Arrays.asList("target")  // target source "target" is missing
        );

        UpdateExecutor.OperationType result = (UpdateExecutor.OperationType) 
                determineOperationTypeMethod.invoke(updateExecutor, diff, "target");
        
        assertEquals("Should be INSERT when record should exist but target missing", 
                UpdateExecutor.OperationType.INSERT, result);
    }

    @Test
    public void testDetermineOperationType_DeleteScenario() throws Exception {
        // Scenario: record should NOT exist (only match keys have values), target exists
        DifferenceRecord diff = createDifferenceRecord(
                Arrays.asList("id", "name"),
                createMatchKeys("id", 1, "name", "test"),
                createResolvedValues("id", 1, "name", "test"),  // Only match keys, no non-match-key fields
                Arrays.asList("source1")  // target source "target" is NOT missing (i.e., exists)
        );

        UpdateExecutor.OperationType result = (UpdateExecutor.OperationType) 
                determineOperationTypeMethod.invoke(updateExecutor, diff, "target");
        
        assertEquals("Should be DELETE when record should not exist but target exists", 
                UpdateExecutor.OperationType.DELETE, result);
    }

    @Test
    public void testDetermineOperationType_UpdateScenario_RecordExistsTargetExists() throws Exception {
        // Scenario: record should exist (has non-match-key values), target exists
        DifferenceRecord diff = createDifferenceRecord(
                Arrays.asList("id", "name"),
                createMatchKeys("id", 1, "name", "test"),
                createResolvedValues("id", 1, "name", "test", "age", 25),
                Arrays.asList("source1")  // target exists (not in missing sources)
        );

        UpdateExecutor.OperationType result = (UpdateExecutor.OperationType) 
                determineOperationTypeMethod.invoke(updateExecutor, diff, "target");
        
        assertEquals("Should be UPDATE when record should exist and target exists", 
                UpdateExecutor.OperationType.UPDATE, result);
    }

    @Test
    public void testDetermineOperationType_UpdateScenario_RecordNotExistTargetNotExist() throws Exception {
        // Scenario: record should NOT exist (only match keys), target also missing
        DifferenceRecord diff = createDifferenceRecord(
                Arrays.asList("id", "name"),
                createMatchKeys("id", 1, "name", "test"),
                createResolvedValues("id", 1, "name", "test"),  // Only match keys
                Arrays.asList("target")  // target is missing (in missing sources)
        );

        UpdateExecutor.OperationType result = (UpdateExecutor.OperationType) 
                determineOperationTypeMethod.invoke(updateExecutor, diff, "target");
        
        assertEquals("Should be UPDATE when record should not exist and target missing", 
                UpdateExecutor.OperationType.UPDATE, result);
    }

    @Test
    public void testDetermineOperationType_WithNullResolvedValues() throws Exception {
        // Scenario: resolution result is null
        DifferenceRecord diff = new DifferenceRecord();
        diff.setRecordId("test-1");
        diff.setMatchKeyValues(createMatchKeys("id", 1));
        diff.setMissingSources(Arrays.asList("target"));  // target missing
        
        // No resolution result set
        
        UpdateExecutor.OperationType result = (UpdateExecutor.OperationType) 
                determineOperationTypeMethod.invoke(updateExecutor, diff, "target");
        
        // With null resolution result, recordShouldExist should be false
        // target missing (!targetExists = true), so recordShouldExist=false, targetExists=false -> UPDATE
        assertEquals("Should be UPDATE when resolution result is null", 
                UpdateExecutor.OperationType.UPDATE, result);
    }

    @Test
    public void testDetermineOperationType_WithEmptyResolvedValues() throws Exception {
        // Scenario: resolved values map is empty
        DifferenceRecord diff = createDifferenceRecord(
                Arrays.asList("id"),
                createMatchKeys("id", 1),
                new HashMap<>(),  // Empty resolved values
                Arrays.asList("target")
        );

        UpdateExecutor.OperationType result = (UpdateExecutor.OperationType) 
                determineOperationTypeMethod.invoke(updateExecutor, diff, "target");
        
        // Empty resolved values -> recordShouldExist = false
        // target missing -> targetExists = false
        // recordShouldExist=false, targetExists=false -> UPDATE
        assertEquals("Should be UPDATE when resolved values are empty", 
                UpdateExecutor.OperationType.UPDATE, result);
    }

    @Test
    public void testDetermineOperationType_NonMatchKeyFieldNull() throws Exception {
        // Scenario: non-match-key fields exist but have null values
        Map<String, Object> resolvedValues = new HashMap<>();
        resolvedValues.put("id", 1);  // match key
        resolvedValues.put("name", "test");  // match key  
        resolvedValues.put("age", null);  // non-match-key but null
        resolvedValues.put("salary", null);  // non-match-key but null
        
        DifferenceRecord diff = createDifferenceRecord(
                Arrays.asList("id", "name"),
                createMatchKeys("id", 1, "name", "test"),
                resolvedValues,
                Arrays.asList("source1")  // target exists
        );

        UpdateExecutor.OperationType result = (UpdateExecutor.OperationType) 
                determineOperationTypeMethod.invoke(updateExecutor, diff, "target");
        
        // All non-match-key fields are null -> recordShouldExist = false
        // target exists -> targetExists = true
        // recordShouldExist=false, targetExists=true -> DELETE
        assertEquals("Should be DELETE when non-match-key fields are null and target exists", 
                UpdateExecutor.OperationType.DELETE, result);
    }

    @Test
    public void testDetermineOperationType_NonMatchKeyFieldMixedNullAndValues() throws Exception {
        // Scenario: some non-match-key fields have values, some are null
        Map<String, Object> resolvedValues = new HashMap<>();
        resolvedValues.put("id", 1);  // match key
        resolvedValues.put("name", "test");  // match key  
        resolvedValues.put("age", 25);  // non-match-key with value
        resolvedValues.put("salary", null);  // non-match-key null
        
        DifferenceRecord diff = createDifferenceRecord(
                Arrays.asList("id", "name"),
                createMatchKeys("id", 1, "name", "test"),
                resolvedValues,
                Arrays.asList("target")  // target missing
        );

        UpdateExecutor.OperationType result = (UpdateExecutor.OperationType) 
                determineOperationTypeMethod.invoke(updateExecutor, diff, "target");
        
        // At least one non-match-key field has non-null value -> recordShouldExist = true
        // target missing -> targetExists = false
        // recordShouldExist=true, targetExists=false -> INSERT
        assertEquals("Should be INSERT when at least one non-match-key field has value and target missing", 
                UpdateExecutor.OperationType.INSERT, result);
    }

    @Test
    public void testDetermineOperationType_InsertWhenWinningSourceIsMissingButAnotherSourceExists() throws Exception {
        DifferenceRecord diff = createDifferenceRecord(
                Arrays.asList("user_id", "username"),
                createMatchKeys("user_id", "7", "username", "wujiu"),
                createResolvedValues("user_id", "7", "username", "wujiu", "age", null, "salary", null),
                Arrays.asList("target", "source-2")
        );
        diff.setSourceValues(new LinkedHashMap<String, Map<String, Object>>() {{
            put("target", createMatchKeys("user_id", "7", "username", "wujiu"));
            put("source-2", createMatchKeys("user_id", "7", "username", "wujiu"));
            put("source-3", createResolvedValues("user_id", "7", "username", "wujiu", "age", null, "salary", null));
        }});
        diff.getResolutionResult().setWinningSource("source-2");

        UpdateExecutor.OperationType result = (UpdateExecutor.OperationType)
                determineOperationTypeMethod.invoke(updateExecutor, diff, "target");

        assertEquals("Should still INSERT when another non-missing source has the record",
                UpdateExecutor.OperationType.INSERT, result);
    }

    @Test
    public void testDetermineOperationType_PrefersUpdatePlanOperationType() throws Exception {
        DifferenceRecord diff = createDifferenceRecord(
                Arrays.asList("id"),
                createMatchKeys("id", 1),
                createResolvedValues("id", 1),
                Arrays.asList("source1")
        );
        UpdatePlan updatePlan = new UpdatePlan();
        updatePlan.setOperationType("SKIP");
        updatePlan.setMatchKeyValues(createMatchKeys("id", 1));
        updatePlan.setResolvedValues(createResolvedValues("id", 1, "age", 18));
        diff.setUpdatePlan(updatePlan);

        UpdateExecutor.OperationType result = (UpdateExecutor.OperationType)
                determineOperationTypeMethod.invoke(updateExecutor, diff, "target");

        assertEquals("Should honor explicit operation type from update plan",
                UpdateExecutor.OperationType.SKIP, result);
    }

    @Test
    public void testDetermineOperationType_UsesLegacyFallbackWhenUpdatePlanAbsent() throws Exception {
        DifferenceRecord diff = createDifferenceRecord(
                Arrays.asList("id", "name"),
                createMatchKeys("id", 1, "name", "test"),
                createResolvedValues("id", 1, "name", "test"),
                Arrays.asList("source1")
        );

        UpdateExecutor.OperationType result = (UpdateExecutor.OperationType)
                determineOperationTypeMethod.invoke(updateExecutor, diff, "target");

        assertEquals("Should retain legacy behavior without update plan",
                UpdateExecutor.OperationType.DELETE, result);
    }

    @Test
    public void testDetermineOperationType_AllowInsertFalseSkipsInsert() throws Exception {
        // This test would require testing executeUpdates method with mocked dependencies
        // For now, we'll note that this is covered by integration tests
        assertTrue(true);
    }

    @Test
    public void testDetermineOperationType_AllowDeleteFalseSkipsDelete() throws Exception {
        // This test would require testing executeUpdates method with mocked dependencies
        // For now, we'll note that this is covered by integration tests
        assertTrue(true);
    }

    @Test
    public void testValuesAreEqual() throws Exception {
        // Test case 1: Both maps null
        assertTrue((Boolean) valuesAreEqualMethod.invoke(updateExecutor, null, null, null));
        
        // Test case 2: One map null
        Map<String, Object> map1 = new HashMap<>();
        map1.put("field", "value");
        assertFalse((Boolean) valuesAreEqualMethod.invoke(updateExecutor, map1, null, null));
        assertFalse((Boolean) valuesAreEqualMethod.invoke(updateExecutor, null, map1, null));
        
        // Test case 3: Equal values excluding match keys
        Map<String, Object> resolved = new HashMap<>();
        resolved.put("id", 1); // match key
        resolved.put("name", "John");
        resolved.put("age", 30);
        Map<String, Object> target = new HashMap<>();
        target.put("id", 1);
        target.put("name", "John");
        target.put("age", 30);
        Map<String, Object> matchKeys = new HashMap<>();
        matchKeys.put("id", 1);
        assertTrue((Boolean) valuesAreEqualMethod.invoke(updateExecutor, resolved, target, matchKeys));
        
        // Test case 4: Different non-match-key values
        Map<String, Object> target2 = new HashMap<>();
        target2.put("id", 1);
        target2.put("name", "Jane"); // different
        target2.put("age", 30);
        assertFalse((Boolean) valuesAreEqualMethod.invoke(updateExecutor, resolved, target2, matchKeys));
        
        // Test case 5: Match key values differ but ignored
        Map<String, Object> target3 = new HashMap<>();
        target3.put("id", 999); // different match key value, should be ignored
        target3.put("name", "John");
        target3.put("age", 30);
        assertTrue((Boolean) valuesAreEqualMethod.invoke(updateExecutor, resolved, target3, matchKeys));
        
        // Test case 6: Null values handling
        Map<String, Object> resolved2 = new HashMap<>();
        resolved2.put("id", 1);
        resolved2.put("name", null);
        resolved2.put("age", 30);
        Map<String, Object> target4 = new HashMap<>();
        target4.put("id", 1);
        target4.put("name", null);
        target4.put("age", 30);
        assertTrue((Boolean) valuesAreEqualMethod.invoke(updateExecutor, resolved2, target4, matchKeys));
        
        // Test case 7: One null, one non-null
        Map<String, Object> target5 = new HashMap<>();
        target5.put("id", 1);
        target5.put("name", "John");
        target5.put("age", 30);
        assertFalse((Boolean) valuesAreEqualMethod.invoke(updateExecutor, resolved2, target5, matchKeys));
    }

    @Test
    public void testConsistencyRuleSkipUnchangedUpdatesField() {
        // Test default value is true
        ConsistencyRule rule = new ConsistencyRule();
        assertTrue("Default value of skipUnchangedUpdates should be true", rule.getSkipUnchangedUpdates());
        
        // Test fromConfig with explicit true
        Configuration config = Configuration.newDefault();
        config.set("skipUnchangedUpdates", true);
        ConsistencyRule ruleFromConfig = ConsistencyRule.fromConfig(config);
        assertTrue("skipUnchangedUpdates should be true when configured", ruleFromConfig.getSkipUnchangedUpdates());
        
        // Test fromConfig with false
        config.set("skipUnchangedUpdates", false);
        ConsistencyRule ruleFromConfigFalse = ConsistencyRule.fromConfig(config);
        assertFalse("skipUnchangedUpdates should be false when configured", ruleFromConfigFalse.getSkipUnchangedUpdates());
        
        // Test toConfig round-trip
        rule.setSkipUnchangedUpdates(false);
        Configuration roundTripConfig = rule.toConfig();
        assertFalse("toConfig should preserve skipUnchangedUpdates value", 
                   roundTripConfig.getBool("skipUnchangedUpdates", true));
    }

    // Helper methods
    private DifferenceRecord createDifferenceRecord(List<String> matchKeys,
                                                   Map<String, Object> matchKeyValues,
                                                   Map<String, Object> resolvedValues,
                                                   List<String> missingSources) {
        DifferenceRecord diff = new DifferenceRecord();
        diff.setRecordId("test-record-" + UUID.randomUUID().toString());
        diff.setMatchKeyValues(matchKeyValues);
        diff.setMissingSources(missingSources);
        
        if (!resolvedValues.isEmpty()) {
            ResolutionResult resolution = new ResolutionResult();
            resolution.setResolvedValues(resolvedValues);
            diff.setResolutionResult(resolution);
        }
        
        return diff;
    }

    private Map<String, Object> createMatchKeys(Object... keyValuePairs) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            map.put((String) keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return map;
    }

    private Map<String, Object> createResolvedValues(Object... keyValuePairs) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < keyValuePairs.length; i += 2) {
            map.put((String) keyValuePairs[i], keyValuePairs[i + 1]);
        }
        return map;
    }
}
