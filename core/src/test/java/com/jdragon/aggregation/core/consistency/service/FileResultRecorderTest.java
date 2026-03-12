package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.OutputConfig;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.*;

public class FileResultRecorderTest {

    private FileResultRecorder recorder;
    private String testOutputDir;

    @Before
    public void setUp() {
        testOutputDir = "./test-output-" + System.currentTimeMillis();
        recorder = new FileResultRecorder(testOutputDir);
    }

    @Test
    public void testGenerateReportWithFreemarker() {
        ComparisonResult result = createTestComparisonResult();
        List<DifferenceRecord> differences = createTestDifferences();
        OutputConfig outputConfig = new OutputConfig();
        outputConfig.setReportLanguage(OutputConfig.ReportLanguage.ENGLISH);
        outputConfig.setGenerateReport(true);

        String reportPath = recorder.generateReport(result, differences, outputConfig);
        
        assertNotNull("Report path should not be null", reportPath);
        assertTrue("Report file should exist", new File(reportPath).exists());
        
        try {
            String htmlContent = new String(Files.readAllBytes(Paths.get(reportPath)));
            assertTrue("HTML should contain report title", htmlContent.contains("Data Consistency Report"));
            assertTrue("HTML should contain rule ID", htmlContent.contains("test-rule-123"));
            assertTrue("HTML should contain differences header", htmlContent.contains("Differences Found"));
            
            // Resolved rows are no longer displayed in HTML reports (removed for simplification)
            // Ensure no resolved rows section appears
            assertFalse("HTML should not contain resolved rows section (removed in v1.2)",
                htmlContent.contains("Resolved Rows"));
        } catch (Exception e) {
            fail("Failed to read report file: " + e.getMessage());
        }
    }



    @Test
    public void testGenerateReportDoesNotShowResolvedRows() {
        ComparisonResult result = createTestComparisonResult();
        // Add some resolved rows (they should not appear in HTML report)
        Map<String, Object> resolvedRow1 = new HashMap<>();
        resolvedRow1.put("user_id", 1001);
        resolvedRow1.put("username", "resolved_user");
        resolvedRow1.put("age", 30);
        result.getResolvedRows().add(resolvedRow1);

        List<DifferenceRecord> differences = createTestDifferences();
        OutputConfig outputConfig = new OutputConfig();
        outputConfig.setReportLanguage(OutputConfig.ReportLanguage.ENGLISH);

        String reportPath = recorder.generateReport(result, differences, outputConfig);
        
        assertNotNull("Report path should not be null", reportPath);
        
        try {
            String htmlContent = new String(Files.readAllBytes(Paths.get(reportPath)));
            // Check that resolved rows section does NOT appear (removed in v1.2)
            assertFalse("HTML should NOT contain resolved rows section (removed in v1.2)", 
                htmlContent.contains("Resolved Rows"));
            // Resolved user data should not appear in the report
            assertFalse("HTML should NOT contain resolved user data (resolved rows removed)", 
                htmlContent.contains("resolved_user"));
        } catch (Exception e) {
            fail("Failed to read report file: " + e.getMessage());
        }
    }

    @Test
    public void testRecordComparisonResult() {
        ComparisonResult result = createTestComparisonResult();
        recorder.recordComparisonResult(result);
        
        // Check that JSON file was created
        File outputDir = new File(testOutputDir);
        File[] jsonFiles = outputDir.listFiles((dir, name) -> name.startsWith("comparison_result_"));
        assertNotNull("JSON files should exist", jsonFiles);
        assertTrue("At least one JSON file should be created", jsonFiles.length > 0);
    }

    @Test
    public void testRecordDifferences() {
        List<DifferenceRecord> differences = createTestDifferences();
        recorder.recordDifferences(differences);
        
        File outputDir = new File(testOutputDir);
        File[] jsonFiles = outputDir.listFiles((dir, name) -> name.startsWith("differences_"));
        assertNotNull("Differences JSON files should exist", jsonFiles);
        assertTrue("At least one differences JSON file should be created", jsonFiles.length > 0);
    }

    private ComparisonResult createTestComparisonResult() {
        ComparisonResult result = new ComparisonResult();
        result.setRuleId("test-rule-123");
        result.setTotalRecords(100);
        result.setConsistentRecords(85);
        result.setInconsistentRecords(15);
        result.setResolvedRecords(10);
        result.setStatus(ComparisonResult.Status.PARTIAL_SUCCESS);
        result.setExecutionTime(new Date());
        
        Map<String, Integer> fieldDiscrepancies = new HashMap<>();
        fieldDiscrepancies.put("age", 8);
        fieldDiscrepancies.put("salary", 7);
        result.setFieldDiscrepancies(fieldDiscrepancies);
        
        return result;
    }

    private List<DifferenceRecord> createTestDifferences() {
        List<DifferenceRecord> differences = new ArrayList<>();
        
        // Create first difference record
        DifferenceRecord diff1 = new DifferenceRecord();
        diff1.setRecordId("diff-001");
        diff1.setConflictType("VALUE_MISMATCH");
        diff1.setDiscrepancyScore(0.75);
        
        Map<String, Object> matchKeys = new HashMap<>();
        matchKeys.put("user_id", 1001);
        matchKeys.put("username", "john_doe");
        diff1.setMatchKeyValues(matchKeys);
        
        Map<String, String> diffMap = new HashMap<>();
        diffMap.put("age", "primary:25, backup:30, warehouse:25");
        diffMap.put("salary", "primary:50000, backup:52000, warehouse:51000");
        diff1.setDifferences(diffMap);
        
        // Add resolution result
        ResolutionResult resolution = new ResolutionResult();
        resolution.setStrategyUsed(com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy.HIGH_CONFIDENCE);
        resolution.setWinningSource("primary");
        
        Map<String, Object> resolvedValues = new HashMap<>();
        resolvedValues.put("age", 25);
        resolvedValues.put("salary", 50000);
        resolution.setResolvedValues(resolvedValues);
        
        diff1.setResolutionResult(resolution);
        differences.add(diff1);
        
        // Create second difference record without resolution
        DifferenceRecord diff2 = new DifferenceRecord();
        diff2.setRecordId("diff-002");
        diff2.setConflictType("MISSING_DATA");
        diff2.setDiscrepancyScore(1.0);
        
        Map<String, Object> matchKeys2 = new HashMap<>();
        matchKeys2.put("user_id", 1002);
        matchKeys2.put("username", "jane_smith");
        diff2.setMatchKeyValues(matchKeys2);
        
        Map<String, String> diffMap2 = new HashMap<>();
        diffMap2.put("department", "primary:Engineering, backup:null, warehouse:Engineering");
        diff2.setDifferences(diffMap2);
        
        differences.add(diff2);
        
        return differences;
    }
}