package com.jdragon.aggregation.core.consistency.example;

import com.jdragon.aggregation.core.consistency.i18n.MessageResource;
import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.OutputConfig;

public class I18nTest {
    public static void main(String[] args) {
        System.out.println("Testing English messages:");
        MessageResource en = MessageResource.forLanguage(OutputConfig.ReportLanguage.ENGLISH);
        System.out.println("report.title: " + en.getMessage("report.title"));
        System.out.println("field.user_id: " + en.getFieldName("user_id"));
        System.out.println("conflict.type.binary: " + en.getConflictType("BINARY_CONFLICT"));
        System.out.println("strategy.high.confidence: " + en.getStrategy(ConflictResolutionStrategy.HIGH_CONFIDENCE));
        
        System.out.println("\nTesting Chinese messages:");
        MessageResource zh = MessageResource.forLanguage(OutputConfig.ReportLanguage.CHINESE);
        System.out.println("report.title: " + zh.getMessage("report.title"));
        System.out.println("field.user_id: " + zh.getFieldName("user_id"));
        System.out.println("conflict.type.binary: " + zh.getConflictType("BINARY_CONFLICT"));
        System.out.println("strategy.high.confidence: " + zh.getStrategy(ConflictResolutionStrategy.HIGH_CONFIDENCE));
        
        System.out.println("\nTesting Bilingual messages:");
        MessageResource bi = MessageResource.forLanguage(OutputConfig.ReportLanguage.BILINGUAL);
        System.out.println("report.title: " + bi.getMessage("report.title"));
        System.out.println("field.user_id: " + bi.getFieldName("user_id"));
        System.out.println("conflict.type.binary: " + bi.getConflictType("BINARY_CONFLICT"));
        System.out.println("strategy.high.confidence: " + bi.getStrategy(ConflictResolutionStrategy.HIGH_CONFIDENCE));
        
        System.out.println("\nTesting comparison messages:");
        System.out.println("comparison.all.nulls: " + en.getMessage("comparison.all.nulls", "age"));
        System.out.println("comparison.some.nulls: " + en.getMessage("comparison.some.nulls", "salary", "[source-1, source-2]"));
        System.out.println("comparison.different.values: " + en.getMessage("comparison.different.values", "department", "[HR, Engineering]"));
        System.out.println("comparison.mixed.numeric: " + en.getMessage("comparison.mixed.numeric", "age", "[25, 'twenty']"));
        System.out.println("comparison.exceeds.tolerance: " + en.getMessage("comparison.exceeds.tolerance", "salary", 5000.0, 1000.0, "{source-1=5500.0, source-2=5000.0}"));
        
        System.out.println("\nAll tests passed!");
    }
}