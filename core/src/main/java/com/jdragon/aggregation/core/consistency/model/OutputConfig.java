package com.jdragon.aggregation.core.consistency.model;

import com.jdragon.aggregation.commons.util.Configuration;
import lombok.Data;

@Data
public class OutputConfig {

    private OutputType outputType = OutputType.FILE;

    private String outputPath;

    private String databaseTable;

    private String databaseConnection;

    private Boolean storeDifferences = true;

    private Boolean storeResolutionResults = true;

    private Boolean generateReport = true;

    private ReportFormat reportFormat = ReportFormat.JSON;

    private ReportLanguage reportLanguage = ReportLanguage.ENGLISH;

    private Integer maxDifferencesToDisplay = 100;

    public static OutputConfig fromConfig(Configuration config) {
        OutputConfig outputConfig = new OutputConfig();
        if (config == null) {
            return outputConfig;
        }
        outputConfig.setOutputType(OutputType.valueOf(config.getString("outputType", "FILE").toUpperCase()));
        outputConfig.setOutputPath(config.getString("outputPath"));
        outputConfig.setDatabaseTable(config.getString("databaseTable"));
        outputConfig.setDatabaseConnection(config.getString("databaseConnection"));
        outputConfig.setStoreDifferences(config.getBool("storeDifferences", true));
        outputConfig.setStoreResolutionResults(config.getBool("storeResolutionResults", true));
        outputConfig.setGenerateReport(config.getBool("generateReport", true));
        outputConfig.setReportFormat(ReportFormat.valueOf(config.getString("reportFormat", "JSON").toUpperCase()));
        outputConfig.setReportLanguage(ReportLanguage.valueOf(config.getString("reportLanguage", "ENGLISH").toUpperCase()));
        outputConfig.setMaxDifferencesToDisplay(config.getInt("maxDifferencesToDisplay", 100));
        return outputConfig;
    }

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
