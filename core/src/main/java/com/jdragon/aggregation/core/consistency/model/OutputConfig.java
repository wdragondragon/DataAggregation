package com.jdragon.aggregation.core.consistency.model;

import lombok.Data;

@Data
public class OutputConfig {
    
    private OutputType outputType; // 输出类型：文件、数据库、内存
    
    private String outputPath; // 输出文件路径（当输出类型为FILE时）
    
    private String databaseTable; // 数据库表名（当输出类型为DATABASE时）
    
    private String databaseConnection; // 数据库连接（当输出类型为DATABASE时）
    
    private Boolean storeDifferences = true; // 是否存储差异数据
    
    private Boolean storeResolutionResults = true; // 是否存储解决结果
    
    private Boolean generateReport = true; // 是否生成报告
    
    private ReportFormat reportFormat = ReportFormat.JSON; // 报告格式：JSON、HTML、CSV
    
    private ReportLanguage reportLanguage = ReportLanguage.ENGLISH; // 报告语言：英文、中文、双语
    
    private Integer maxDifferencesToDisplay = 100; // 报告中最大显示差异数量
    
    public enum OutputType {
        FILE, // 文件输出
        DATABASE, // 数据库输出
        MEMORY // 内存输出
    }
    
    public enum ReportFormat {
        JSON, // JSON格式报告
        HTML, // HTML格式报告
        CSV // CSV格式报告
    }
    
    public enum ReportLanguage {
        ENGLISH, // 英文报告
        CHINESE, // 中文报告
        BILINGUAL // 双语报告（中英文）
    }
}