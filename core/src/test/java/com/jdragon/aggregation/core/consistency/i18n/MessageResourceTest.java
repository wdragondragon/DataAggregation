package com.jdragon.aggregation.core.consistency.i18n;

import com.jdragon.aggregation.core.consistency.model.ConflictResolutionStrategy;
import com.jdragon.aggregation.core.consistency.model.OutputConfig;
import org.junit.Test;

import static org.junit.Assert.*;

public class MessageResourceTest {

    @Test
    public void testEnglishMessages() {
        MessageResource messages = MessageResource.forLanguage(OutputConfig.ReportLanguage.ENGLISH);
        
        assertEquals("Data Consistency Report", messages.getMessage("report.title"));
        assertEquals("Summary", messages.getMessage("report.summary"));
        assertEquals("Rule ID", messages.getMessage("metric.rule.id"));
        assertEquals("Differences Found: 5", messages.getMessage("report.differences.found", 5));
        assertEquals("User ID", messages.getFieldName("user_id"));
        assertEquals("Age", messages.getFieldName("age"));
        assertEquals("Binary Conflict", messages.getConflictType("BINARY_CONFLICT"));
        assertEquals("Multi-source Conflict", messages.getConflictType("MULTI_SOURCE_CONFLICT"));
        assertEquals("High Confidence Source", messages.getStrategy(ConflictResolutionStrategy.HIGH_CONFIDENCE));
        assertEquals("Weighted Average", messages.getStrategy(ConflictResolutionStrategy.WEIGHTED_AVERAGE));
    }

    @Test
    public void testChineseMessages() {
        MessageResource messages = MessageResource.forLanguage(OutputConfig.ReportLanguage.CHINESE);
        
        assertEquals("数据一致性报告", messages.getMessage("report.title"));
        assertEquals("报告摘要", messages.getMessage("report.summary"));
        assertEquals("规则ID", messages.getMessage("metric.rule.id"));
        assertEquals("发现差异: 5 处", messages.getMessage("report.differences.found", 5));
        assertEquals("用户ID", messages.getFieldName("user_id"));
        assertEquals("年龄", messages.getFieldName("age"));
        assertEquals("二元冲突", messages.getConflictType("BINARY_CONFLICT"));
        assertEquals("多源冲突", messages.getConflictType("MULTI_SOURCE_CONFLICT"));
        assertEquals("高可信度源", messages.getStrategy(ConflictResolutionStrategy.HIGH_CONFIDENCE));
        assertEquals("加权平均值", messages.getStrategy(ConflictResolutionStrategy.WEIGHTED_AVERAGE));
    }

    @Test
    public void testBilingualMessages() {
        MessageResource messages = MessageResource.forLanguage(OutputConfig.ReportLanguage.BILINGUAL);
        
        // Bilingual format: English / Chinese
        assertEquals("Data Consistency Report / 数据一致性报告", messages.getMessage("report.title"));
        assertEquals("Summary / 报告摘要", messages.getMessage("report.summary"));
        assertEquals("Rule ID / 规则ID", messages.getMessage("metric.rule.id"));
        assertEquals("Differences Found: 5 / 发现差异: 5 处", messages.getMessage("report.differences.found", 5));
        assertEquals("User ID / 用户ID", messages.getFieldName("user_id"));
        assertEquals("Age / 年龄", messages.getFieldName("age"));
        assertEquals("Binary Conflict / 二元冲突", messages.getConflictType("BINARY_CONFLICT"));
        assertEquals("Multi-source Conflict / 多源冲突", messages.getConflictType("MULTI_SOURCE_CONFLICT"));
        assertEquals("High Confidence Source / 高可信度源", messages.getStrategy(ConflictResolutionStrategy.HIGH_CONFIDENCE));
        assertEquals("Weighted Average / 加权平均值", messages.getStrategy(ConflictResolutionStrategy.WEIGHTED_AVERAGE));
    }

    @Test
    public void testFieldNameFallback() {
        MessageResource messages = MessageResource.forLanguage(OutputConfig.ReportLanguage.ENGLISH);
        // Unknown field returns formatted field name
        assertEquals("Unknown Field", messages.getFieldName("unknown_field"));
        assertEquals("Unknown Field / unknown_field", 
                MessageResource.forLanguage(OutputConfig.ReportLanguage.BILINGUAL).getFieldName("unknown_field"));
    }

    @Test
    public void testConflictTypeFallback() {
        MessageResource messages = MessageResource.forLanguage(OutputConfig.ReportLanguage.ENGLISH);
        // Unknown conflict type returns original
        assertEquals("UNKNOWN_TYPE", messages.getConflictType("UNKNOWN_TYPE"));
    }

    @Test
    public void testComparisonMessages() {
        // Test English comparison messages
        MessageResource english = MessageResource.forLanguage(OutputConfig.ReportLanguage.ENGLISH);
        assertEquals("All sources have null values for field 'age'", 
            english.getMessage("comparison.all.nulls", "age"));
        assertEquals("Some sources have null values for field 'salary': [source1, source2]", 
            english.getMessage("comparison.some.nulls", "salary", "[source1, source2]"));
        assertEquals("Field 'department' has different values across sources", 
            english.getMessage("comparison.different.values", "department"));
        assertEquals("Field 'amount' has mixed numeric and non-numeric values: {source1=100, source2=invalid}", 
            english.getMessage("comparison.mixed.numeric", "amount", "{source1=100, source2=invalid}"));
        assertEquals("Field 'score' exceeds tolerance threshold (0.1500 > 0.1000). Values: {source1=1.5, source2=1.2}", 
            english.getMessage("comparison.exceeds.tolerance", "score", 0.15, 0.1, "{source1=1.5, source2=1.2}"));

        // Test Chinese comparison messages
        MessageResource chinese = MessageResource.forLanguage(OutputConfig.ReportLanguage.CHINESE);
        assertEquals("所有数据源字段'age'均为空值", 
            chinese.getMessage("comparison.all.nulls", "age"));
        assertEquals("部分数据源字段'salary'为空值: [source1, source2]", 
            chinese.getMessage("comparison.some.nulls", "salary", "[source1, source2]"));
        assertEquals("字段'department'在不同数据源中值不一致", 
            chinese.getMessage("comparison.different.values", "department"));
        assertEquals("字段'amount'混合了数字和非数字值: {source1=100, source2=invalid}", 
            chinese.getMessage("comparison.mixed.numeric", "amount", "{source1=100, source2=invalid}"));
        assertEquals("字段'score'超过容差阈值(0.1500 > 0.1000)。值: {source1=1.5, source2=1.2}", 
            chinese.getMessage("comparison.exceeds.tolerance", "score", 0.15, 0.1, "{source1=1.5, source2=1.2}"));

        // Test Bilingual comparison messages
        MessageResource bilingual = MessageResource.forLanguage(OutputConfig.ReportLanguage.BILINGUAL);
        assertEquals("All sources have null values for field 'age' / 所有数据源字段'age'均为空值", 
            bilingual.getMessage("comparison.all.nulls", "age"));
        assertEquals("Some sources have null values for field 'salary': [source1, source2] / 部分数据源字段'salary'为空值: [source1, source2]", 
            bilingual.getMessage("comparison.some.nulls", "salary", "[source1, source2]"));
        assertEquals("Field 'department' has different values across sources / 字段'department'在不同数据源中值不一致", 
            bilingual.getMessage("comparison.different.values", "department"));
        assertEquals("Field 'amount' has mixed numeric and non-numeric values: {source1=100, source2=invalid} / 字段'amount'混合了数字和非数字值: {source1=100, source2=invalid}", 
            bilingual.getMessage("comparison.mixed.numeric", "amount", "{source1=100, source2=invalid}"));
        assertEquals("Field 'score' exceeds tolerance threshold (0.1500 > 0.1000). Values: {source1=1.5, source2=1.2} / 字段'score'超过容差阈值(0.1500 > 0.1000)。值: {source1=1.5, source2=1.2}", 
            bilingual.getMessage("comparison.exceeds.tolerance", "score", 0.15, 0.1, "{source1=1.5, source2=1.2}"));
    }
}