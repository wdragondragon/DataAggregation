package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.OutputConfig;

import java.util.List;

public interface ResultRecorder {
    
    void recordComparisonResult(ComparisonResult result);
    
    void recordDifferences(List<DifferenceRecord> differences);
    
    void recordResolutionResults(List<DifferenceRecord> resolvedDifferences);
    
    String generateReport(ComparisonResult result, List<DifferenceRecord> differences);
    
    String generateReport(ComparisonResult result, List<DifferenceRecord> differences, OutputConfig outputConfig);
}