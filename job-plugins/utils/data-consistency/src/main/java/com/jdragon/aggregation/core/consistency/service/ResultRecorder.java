package com.jdragon.aggregation.core.consistency.service;

import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.OutputConfig;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;

import java.util.List;

public interface ResultRecorder {
    
    void recordComparisonResult(ComparisonResult result);
    
    String recordDifferences(List<DifferenceRecord> differences);

    List<ResolutionResult> recordResolutionResults(ComparisonResult result, List<DifferenceRecord> resolvedDifferences);
    
    String generateReport(ComparisonResult result, List<DifferenceRecord> differences);
    
    String generateReport(ComparisonResult result, List<DifferenceRecord> differences, OutputConfig outputConfig);
}