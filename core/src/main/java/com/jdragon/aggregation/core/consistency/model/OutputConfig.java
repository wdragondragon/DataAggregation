package com.jdragon.aggregation.core.consistency.model;

import lombok.Data;

@Data
public class OutputConfig {
    
    private OutputType outputType;
    
    private String outputPath;
    
    private String databaseTable;
    
    private String databaseConnection;
    
    private Boolean storeDifferences = true;
    
    private Boolean storeResolutionResults = true;
    
    private Boolean generateReport = true;
    
    private ReportFormat reportFormat = ReportFormat.JSON;
    
    private ReportLanguage reportLanguage = ReportLanguage.ENGLISH;
    
    private Integer maxDifferencesToDisplay = 100;
    
    public enum OutputType {
        FILE,
        DATABASE,
        MEMORY
    }
    
    public enum ReportFormat {
        JSON,
        HTML,
        CSV
    }
    
    public enum ReportLanguage {
        ENGLISH,
        CHINESE,
        BILINGUAL
    }
}