package com.jdragon.aggregation.core.consistency.model;

import com.jdragon.aggregation.commons.util.Configuration;
import lombok.Data;

@Data
public class OutputConfig {

    private String outputPath;

    private Boolean generateReport = true;

    private Boolean storeDifferences = true;

    private Boolean storeResolutionResults = true;

    private ReportLanguage reportLanguage = ReportLanguage.ENGLISH;

    private Integer maxDifferencesToDisplay = 100;

    public static OutputConfig fromConfig(Configuration config) {
        OutputConfig outputConfig = new OutputConfig();
        if (config == null) {
            return outputConfig;
        }
        outputConfig.setOutputPath(config.getString("outputPath"));
        outputConfig.setGenerateReport(config.getBool("generateReport", true));
        outputConfig.setStoreDifferences(config.getBool("storeDifferences", true));
        outputConfig.setStoreResolutionResults(config.getBool("storeResolutionResults", true));
        outputConfig.setReportLanguage(ReportLanguage.valueOf(config.getString("reportLanguage", "ENGLISH").toUpperCase()));
        outputConfig.setMaxDifferencesToDisplay(config.getInt("maxDifferencesToDisplay", 100));
        return outputConfig;
    }

    public enum ReportLanguage {
        ENGLISH,
        CHINESE,
        BILINGUAL
    }
}
