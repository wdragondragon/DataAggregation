package com.jdragon.aggregation.core.consistency.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.jdragon.aggregation.core.consistency.i18n.MessageResource;
import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.OutputConfig;
import com.jdragon.aggregation.core.consistency.model.ResolutionResult;
import com.jdragon.aggregation.core.consistency.model.UpdateResult;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

import static com.jdragon.aggregation.core.consistency.model.OutputConfig.ReportLanguage;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.io.StringWriter;

@Slf4j
public class FileResultRecorder implements ResultRecorder {

    private final String outputDirectory;

    private final SimpleDateFormat reportDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Configuration freemarkerConfig;

    public FileResultRecorder(String outputDirectory) {
        this.outputDirectory = outputDirectory;
        ensureOutputDirectory();
        initFreemarkerConfig();
    }

    @Override
    public void recordComparisonResult(ComparisonResult result) {
        try {
            String timestamp = reportDateFormat.format(new Date());
            String filename = String.format("comparison_result_%s_%s.json",
                    result.getRuleId(), timestamp);

            Path filePath = Paths.get(outputDirectory, filename);
            
            // Create simplified comparison result without resolvedRows to save space
            Map<String, Object> simplifiedResult = new HashMap<>();
            simplifiedResult.put("resultId", result.getResultId());
            simplifiedResult.put("ruleId", result.getRuleId());
            simplifiedResult.put("executionTime", result.getExecutionTime());
            simplifiedResult.put("status", result.getStatus());
            simplifiedResult.put("totalRecords", result.getTotalRecords());
            simplifiedResult.put("consistentRecords", result.getConsistentRecords());
            simplifiedResult.put("inconsistentRecords", result.getInconsistentRecords());
            simplifiedResult.put("resolvedRecords", result.getResolvedRecords());
            simplifiedResult.put("fieldDiscrepancies", result.getFieldDiscrepancies());
            simplifiedResult.put("summary", result.getSummary());
            simplifiedResult.put("reportPath", result.getReportPath());
            simplifiedResult.put("metadata", result.getMetadata());
            
            // Include update result if available
            if (result.getUpdateResult() != null) {
                simplifiedResult.put("updateResult", result.getUpdateResult());
            }
            
            // Skip resolvedRows to save space - they're available in resolutions.json

            String json = JSON.toJSONString(simplifiedResult, SerializerFeature.PrettyFormat);
            Files.write(filePath, json.getBytes());
            log.info("Comparison result recorded to: {}", filePath);

            result.setReportPath(filePath.toString());
        } catch (IOException e) {
            log.error("Failed to record comparison result", e);
        }
    }

    @Override
    public void recordDifferences(List<DifferenceRecord> differences) {
        if (differences == null || differences.isEmpty()) {
            return;
        }

        try {
            String timestamp = reportDateFormat.format(new Date());
            String filename = String.format("differences_%s.json", timestamp);

            Path filePath = Paths.get(outputDirectory, filename);
            
            // Create simplified differences for smaller file size
            List<Map<String, Object>> simplifiedDifferences = differences.stream()
                    .map(diff -> {
                        Map<String, Object> simplified = new HashMap<>();
                        simplified.put("recordId", diff.getRecordId());
                        simplified.put("matchKeyValues", diff.getMatchKeyValues());
                        simplified.put("conflictType", diff.getConflictType());
                        simplified.put("differences", diff.getDifferences());
                        simplified.put("discrepancyScore", diff.getDiscrepancyScore());
                        simplified.put("missingSources", diff.getMissingSources());
                        
                        // Include only inconsistent fields from source values to save space
                        if (diff.getSourceValues() != null && !diff.getSourceValues().isEmpty()) {
                            Map<String, Map<String, Object>> inconsistentFields = new HashMap<>();
                            Set<String> inconsistentFieldNames = diff.getDifferences().keySet();
                            
                            for (Map.Entry<String, Map<String, Object>> sourceEntry : diff.getSourceValues().entrySet()) {
                                String sourceId = sourceEntry.getKey();
                                Map<String, Object> sourceVals = sourceEntry.getValue();
                                Map<String, Object> filteredValues = new HashMap<>();
                                
                                for (String field : inconsistentFieldNames) {
                                    if (sourceVals.containsKey(field)) {
                                        filteredValues.put(field, sourceVals.get(field));
                                    }
                                }
                                
                                // Also include match key values for context
                                if (diff.getMatchKeyValues() != null) {
                                    filteredValues.putAll(diff.getMatchKeyValues());
                                }
                                
                                if (!filteredValues.isEmpty()) {
                                    inconsistentFields.put(sourceId, filteredValues);
                                }
                            }
                            
                            if (!inconsistentFields.isEmpty()) {
                                simplified.put("sourceValues", inconsistentFields);
                            }
                        }
                        
                        return simplified;
                    }).collect(Collectors.toList());

            String json = JSON.toJSONString(simplifiedDifferences, SerializerFeature.PrettyFormat);
            Files.write(filePath, json.getBytes());
            log.info("Differences recorded to: {}", filePath);
        } catch (IOException e) {
            log.error("Failed to record differences", e);
        }
    }

    @Override
    public void recordResolutionResults(ComparisonResult result, List<DifferenceRecord> resolvedDifferences) {
        if (resolvedDifferences == null || resolvedDifferences.isEmpty()) {
            return;
        }

        try {
            String timestamp = reportDateFormat.format(new Date());
            String filename = String.format("resolutions_%s.json", timestamp);

            Path filePath = Paths.get(outputDirectory, filename);

            // Get operation types from update result if available
            final Map<String, String> operationTypes = 
                (result != null && result.getUpdateResult() != null) 
                    ? result.getUpdateResult().getOperationTypes() 
                    : null;

            List<Object> resolutions = resolvedDifferences.stream()
                    .map(diff -> {
                        Map<String, Object> resolution = new HashMap<>();
                        ResolutionResult resolutionResult = diff.getResolutionResult();
                        if (resolutionResult != null) {
                            // Convert ResolutionResult to Map using JSON.toJSON
                            Object jsonObj = JSON.toJSON(resolutionResult);
                            if (jsonObj instanceof Map) {
                                resolution.putAll((Map<String, Object>) jsonObj);
                            }
                            
                            // Add operation type if available
                            if (operationTypes != null) {
                                String operationType = operationTypes.get(diff.getRecordId());
                                if (operationType != null) {
                                    resolution.put("operationType", operationType);
                                }
                            }
                            
                            // Add match keys for reference
                            resolution.put("matchKeyValues", diff.getMatchKeyValues());
                            resolution.put("recordId", diff.getRecordId());
                        }
                        return resolution;
                    }).collect(Collectors.toList());

            String json = JSON.toJSONString(resolutions, SerializerFeature.PrettyFormat);
            Files.write(filePath, json.getBytes());
            log.info("Resolution results recorded to: {}", filePath);
        } catch (IOException e) {
            log.error("Failed to record resolution results", e);
        }
    }

    @Override
    public String generateReport(ComparisonResult result, List<DifferenceRecord> differences) {
        return generateReport(result, differences, null);
    }

    @Override
    public String generateReport(ComparisonResult result, List<DifferenceRecord> differences, OutputConfig outputConfig) {
        try {
            String timestamp = reportDateFormat.format(new Date());
            String filename = String.format("consistency_report_%s_%s.html",
                    result.getRuleId(), timestamp);

            Path filePath = Paths.get(outputDirectory, filename);
            String htmlReport = generateHtmlReport(result, differences, outputConfig);

            Files.write(filePath, htmlReport.getBytes(StandardCharsets.UTF_8));
            log.info("HTML report generated: {}", filePath);

            return filePath.toString();
        } catch (IOException e) {
            log.error("Failed to generate report", e);
            return null;
        }
    }

    private String generateHtmlReport(ComparisonResult result, List<DifferenceRecord> differences, OutputConfig outputConfig) {
        // Try to use Freemarker template first for better maintainability and styling
        if (freemarkerConfig != null) {
            try {
                log.info("use freemarker generate html report....");
                return generateHtmlReportWithFreemarker(result, differences, outputConfig);
            } catch (Exception e) {
                log.warn("Freemarker report generation failed, falling back to string concatenation", e);
                // Continue to fallback method
            }
        }

        // Fallback to original string concatenation method
        return generateHtmlReportWithStringConcatenation(result, differences, outputConfig);
    }

    private String generateHtmlReportWithStringConcatenation(ComparisonResult result, List<DifferenceRecord> differences, OutputConfig outputConfig) {
        ReportLanguage language = outputConfig != null ? outputConfig.getReportLanguage() : ReportLanguage.ENGLISH;
        MessageResource messages = MessageResource.forLanguage(language);

        StringBuilder html = new StringBuilder();
        html.append("<!DOCTYPE html>\n");
        html.append("<html>\n");
        html.append("<head>\n");
        html.append("    <meta charset=\"UTF-8\">\n");

        // Set title based on language
        html.append("    <title>").append(messages.getMessage("report.title")).append("</title>\n");

        html.append("    <style>\n");
        html.append("        body { font-family: Arial, sans-serif; margin: 20px; }\n");
        html.append("        .summary { background: #f5f5f5; padding: 15px; border-radius: 5px; }\n");
        html.append("        .metric { display: inline-block; margin-right: 20px; }\n");
        html.append("        .difference { border: 1px solid #ddd; margin: 10px 0; padding: 10px; }\n");
        html.append("        table { border-collapse: collapse; width: 100%; }\n");
        html.append("        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }\n");
        html.append("        th { background-color: #f2f2f2; }\n");
        html.append("        .bilingual { border-left: 3px solid #007bff; padding-left: 10px; margin: 5px 0; }\n");
        html.append("        .chinese { font-family: 'Microsoft YaHei', sans-serif; }\n");

        html.append("        .field-diff { background-color: #fff3cd; padding: 5px; margin: 2px 0; border-left: 3px solid #ffc107; }\n");
        html.append("        .update-stats { background-color: #e8f4fd; padding: 15px; border-radius: 5px; margin: 20px 0; border-left: 4px solid #2196F3; }\n");
        html.append("        .update-success { color: #4CAF50; }\n");
        html.append("        .update-failure { color: #f44336; }\n");
        html.append("        .update-failure-item { background-color: #ffebee; padding: 10px; margin: 5px 0; border-left: 3px solid #f44336; }\n");
        html.append("        .field-with-diff { background-color: #fff3cd; font-weight: bold; }\n");
        html.append("        .field-with-diff-header { background-color: #ffeb3b; color: #000; }\n");
        html.append("    </style>\n");
        html.append("</head>\n");
        html.append("<body");
        if (language == ReportLanguage.CHINESE) {
            html.append(" class=\"chinese\"");
        }
        html.append(">\n");

        // Header based on language
        html.append("<h1>").append(messages.getMessage("report.title")).append("</h1>\n");

        // Summary section with language support
        html.append("<div class=\"summary\">\n");
        html.append("<h2>").append(messages.getMessage("report.summary")).append("</h2>\n");
        html.append("<div class=\"metric\"><strong>").append(messages.getMessage("metric.rule.id")).append(":</strong> ").append(result.getRuleId()).append("</div>\n");
        html.append("<div class=\"metric\"><strong>").append(messages.getMessage("metric.execution.time")).append(":</strong> ").append(dateFormat.format(result.getExecutionTime())).append("</div>\n");
        html.append("<div class=\"metric\"><strong>").append(messages.getMessage("metric.status")).append(":</strong> ").append(result.getStatus()).append("</div>\n");
        html.append("<div class=\"metric\"><strong>").append(messages.getMessage("metric.total.records")).append(":</strong> ").append(result.getTotalRecords()).append("</div>\n");
        html.append("<div class=\"metric\"><strong>").append(messages.getMessage("metric.consistent.records")).append(":</strong> ").append(result.getConsistentRecords()).append("</div>\n");
        html.append("<div class=\"metric\"><strong>").append(messages.getMessage("metric.inconsistent.records")).append(":</strong> ").append(result.getInconsistentRecords()).append("</div>\n");
        html.append("<div class=\"metric\"><strong>").append(messages.getMessage("metric.resolved.records")).append(":</strong> ").append(result.getResolvedRecords()).append("</div>\n");
        
        // Add consistency rate
        if (result.getTotalRecords() > 0) {
            double consistencyRate = (double) result.getConsistentRecords() / result.getTotalRecords() * 100;
            html.append("<div class=\"metric\"><strong>").append(messages.getMessage("metric.consistency.rate")).append(":</strong> ").append(String.format("%.2f", consistencyRate)).append("%</div>\n");
        } else {
            html.append("<div class=\"metric\"><strong>").append(messages.getMessage("metric.consistency.rate")).append(":</strong> N/A</div>\n");
        }
        
        // Add update operation statistics if updates were executed
        if (result.getUpdateResult() != null) {
            UpdateResult updateResult = result.getUpdateResult();
            html.append("<div class=\"metric\"><strong>").append(messages.getMessage("update.target.source")).append(":</strong> ").append(updateResult.getTargetSourceId()).append("</div>\n");
            html.append("<div class=\"metric\"><strong>").append(messages.getMessage("update.total.updates")).append(":</strong> ").append(updateResult.getTotalUpdates()).append("</div>\n");
            html.append("<div class=\"metric\"><strong class=\"update-success\">").append(messages.getMessage("update.successful")).append(":</strong> ").append(updateResult.getSuccessfulUpdates()).append("</div>\n");
            html.append("<div class=\"metric\"><strong class=\"update-failure\">").append(messages.getMessage("update.failed")).append(":</strong> ").append(updateResult.getFailedUpdates()).append("</div>\n");
            html.append("<div class=\"metric\"><strong>").append(messages.getMessage("update.insert.count")).append(":</strong> ").append(updateResult.getInsertCount()).append("</div>\n");
            html.append("<div class=\"metric\"><strong>").append(messages.getMessage("update.update.count")).append(":</strong> ").append(updateResult.getUpdateCount()).append("</div>\n");
            html.append("<div class=\"metric\"><strong>").append(messages.getMessage("update.delete.count")).append(":</strong> ").append(updateResult.getDeleteCount()).append("</div>\n");
            html.append("<div class=\"metric\"><strong>").append(messages.getMessage("update.skip.count")).append(":</strong> ").append(updateResult.getSkipCount()).append("</div>\n");
        }
        
        html.append("</div>\n");



        if (!differences.isEmpty()) {
            // Differences header
            html.append("<h2>").append(messages.getMessage("report.differences.found", differences.size())).append("</h2>\n");
            
            int maxDisplay = outputConfig != null && outputConfig.getMaxDifferencesToDisplay() != null ? outputConfig.getMaxDifferencesToDisplay() : 100;
            for (int i = 0; i < Math.min(differences.size(), maxDisplay); i++) {
                DifferenceRecord diff = differences.get(i);
                html.append("<div class=\"difference\">\n");

                // Difference header
                html.append("<h3>").append(messages.getMessage("difference.header", i + 1)).append("</h3>\n");
                html.append("<p><strong>").append(messages.getMessage("difference.record.id")).append(":</strong> ").append(diff.getRecordId()).append("</p>\n");
                html.append("<p><strong>").append(messages.getMessage("difference.match.keys")).append(":</strong> ").append(JSONObject.toJSONString(diff.getMatchKeyValues())).append("</p>\n");
                html.append("<p><strong>").append(messages.getMessage("difference.conflict.type")).append(":</strong> ").append(messages.getConflictType(diff.getConflictType())).append("</p>\n");
                html.append("<p><strong>").append(messages.getMessage("difference.discrepancy.score")).append(":</strong> ").append(String.format("%.2f", diff.getDiscrepancyScore())).append("</p>\n");

                html.append("<h4>").append(messages.getMessage("difference.differences.field")).append("</h4>\n");
                
                // Build field list: match keys first, then other fields
                List<String> allFields = new ArrayList<>();
                
                // Add match keys
                if (diff.getMatchKeyValues() != null) {
                    for (String key : diff.getMatchKeyValues().keySet()) {
                        if (!allFields.contains(key)) {
                            allFields.add(key);
                        }
                    }
                }
                
                // Add all other fields from first source
                if (diff.getSourceValues() != null && !diff.getSourceValues().isEmpty()) {
                    Map<String, Object> firstSourceValues = diff.getSourceValues().values().iterator().next();
                    if (firstSourceValues != null) {
                        for (String field : firstSourceValues.keySet()) {
                            if (!allFields.contains(field)) {
                                allFields.add(field);
                            }
                        }
                    }
                }
                
                if (diff.getSourceValues() != null && !diff.getSourceValues().isEmpty()) {
                    html.append("<table class=\"data-table\">\n");
                    html.append("<thead>\n");
                    html.append("<tr>\n");
                     html.append("<th>").append(messages.getMessage("difference.data.source")).append("</th>\n");
                     for (String field : allFields) {
                         boolean hasDiff = diff.getDifferences() != null && diff.getDifferences().containsKey(field);
                         html.append("<th");
                         if (hasDiff) {
                             html.append(" class=\"field-with-diff-header\"");
                         }
                         html.append(">").append(field).append("</th>\n");
                     }
                    html.append("</tr>\n");
                    html.append("</thead>\n");
                    html.append("<tbody>\n");
                    
                    // Data source rows
                    for (Map.Entry<String, Map<String, Object>> entry : diff.getSourceValues().entrySet()) {
                        String sourceId = entry.getKey();
                        Map<String, Object> sourceData = entry.getValue();
                        
                        html.append("<tr>\n");
                        html.append("<td>").append(sourceId).append("</td>\n");
                        
                         for (String field : allFields) {
                             boolean hasDiff = diff.getDifferences() != null && diff.getDifferences().containsKey(field);
                             html.append("<td");
                             if (hasDiff) {
                                 html.append(" class=\"field-with-diff\"");
                             }
                             html.append(">");
                             if (sourceData != null && sourceData.containsKey(field) && sourceData.get(field) != null) {
                                 html.append(sourceData.get(field).toString());
                             } else {
                                 html.append("<em>null</em>");
                             }
                             html.append("</td>\n");
                         }
                        html.append("</tr>\n");
                    }
                    
                    // Resolution result row
                    if (diff.getResolutionResult() != null && diff.getResolutionResult().getResolvedValues() != null) {
                        Map<String, Object> resolvedValues = diff.getResolutionResult().getResolvedValues();
                        html.append("<tr class=\"resolution-row\">\n");
                        html.append("<td><strong>").append(messages.getMessage("difference.result.values")).append("</strong></td>\n");
                        
                         for (String field : allFields) {
                             boolean hasDiff = diff.getDifferences() != null && diff.getDifferences().containsKey(field);
                             html.append("<td");
                             if (hasDiff) {
                                 html.append(" class=\"field-with-diff\"");
                             }
                             html.append(">");
                             if (resolvedValues.containsKey(field) && resolvedValues.get(field) != null) {
                                 html.append("<strong>").append(resolvedValues.get(field).toString()).append("</strong>");
                             } else {
                                 html.append("<em>null</em>");
                             }
                             html.append("</td>\n");
                         }
                        html.append("</tr>\n");
                    }
                    
                    html.append("</tbody>\n");
                    html.append("</table>\n");
                }

                if (diff.getResolutionResult() != null) {
                    // Resolution result details (strategy, winning source, time)
                    html.append("<h4>").append(messages.getMessage("difference.resolution.result")).append("</h4>\n");
                    html.append("<p><strong>").append(messages.getMessage("difference.strategy.used")).append(":</strong> ").append(messages.getStrategy(diff.getResolutionResult().getStrategyUsed())).append("</p>\n");
                    if (diff.getResolutionResult().getWinningSource() != null) {
                        html.append("<p><strong>").append(messages.getMessage("difference.winning.source")).append(":</strong> ").append(diff.getResolutionResult().getWinningSource()).append("</p>\n");
                    }
                    // Note: Resolved values are now shown in the table
                    html.append("<p><strong>").append(messages.getMessage("difference.resolution.time")).append(":</strong> ").append(dateFormat.format(diff.getResolutionResult().getResolutionTime())).append("</p>\n");
                }

                html.append("</div>\n");
            }

            if (differences.size() > maxDisplay) {
                html.append("<p>").append(messages.getMessage("report.more.differences", differences.size() - maxDisplay)).append("</p>\n");
            }
        } else {
            html.append("<h2>").append(messages.getMessage("report.no.differences")).append("</h2>\n");
            html.append("<p>").append(messages.getMessage("report.all.consistent")).append("</p>\n");
        }

        // Display update failure details if any failures occurred
        if (result.getUpdateResult() != null && !result.getUpdateResult().getFailures().isEmpty()) {
            UpdateResult updateResult = result.getUpdateResult();
            html.append("<div class=\"update-stats\">\n");
            html.append("<h2>").append(messages.getMessage("update.failure.details")).append(" (").append(updateResult.getFailedUpdates()).append(")</h2>\n");
            
            for (UpdateResult.UpdateFailure failure : updateResult.getFailures()) {
                html.append("<div class=\"update-failure-item\">\n");
                html.append("<p><strong>").append(messages.getMessage("difference.record.id")).append(":</strong> ").append(failure.getRecordId()).append("</p>\n");
                if (failure.getMatchKeys() != null && !failure.getMatchKeys().isEmpty()) {
                    html.append("<p><strong>").append(messages.getMessage("difference.match.keys")).append(":</strong> ").append(JSONObject.toJSONString(failure.getMatchKeys())).append("</p>\n");
                }
                html.append("<p><strong>").append(messages.getMessage("update.failure.reason")).append(":</strong> ").append(failure.getReason()).append("</p>\n");
                html.append("</div>\n");
            }
            html.append("</div>\n");
        }

        html.append("</body>\n");
        html.append("</html>");

        return html.toString();
    }


    private void ensureOutputDirectory() {
        File dir = new File(outputDirectory);
        if (!dir.exists()) {
            boolean created = dir.mkdirs();
            if (created) {
                log.info("Created output directory: {}", outputDirectory);
            } else {
                log.warn("Failed to create output directory: {}", outputDirectory);
            }
        }
    }

    private void initFreemarkerConfig() {
        try {
            Configuration cfg = new Configuration(Configuration.VERSION_2_3_31);
            cfg.setClassForTemplateLoading(FileResultRecorder.class, "/templates");
            cfg.setDefaultEncoding("UTF-8");
            cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
            cfg.setLogTemplateExceptions(false);
            cfg.setWrapUncheckedExceptions(true);
            cfg.setFallbackOnNullLoopVariable(false);
            this.freemarkerConfig = cfg;
            log.info("Freemarker configuration initialized");
        } catch (Exception e) {
            log.warn("Failed to initialize Freemarker configuration, will fallback to string concatenation", e);
            this.freemarkerConfig = null;
        }
    }

    private String generateHtmlReportWithFreemarker(ComparisonResult result,
                                                    List<DifferenceRecord> differences,
                                                    OutputConfig outputConfig) {
        if (freemarkerConfig == null) {
            throw new IllegalStateException("Freemarker configuration not initialized");
        }

        try {
            ReportLanguage language = outputConfig != null ? outputConfig.getReportLanguage() : ReportLanguage.ENGLISH;
            MessageResource messages = MessageResource.forLanguage(language);

            Template template = freemarkerConfig.getTemplate("consistency_report.ftl");

             Map<String, Object> data = new HashMap<>();
             data.put("result", result);
             data.put("differences", differences);
             data.put("messages", messages);
             data.put("dateFormat", dateFormat);
             data.put("language", language.name());
             data.put("jsonHelper", new JsonHelper());
             data.put("outputConfig", outputConfig);

            StringWriter writer = new StringWriter();
            template.process(data, writer);
            return writer.toString();
        } catch (IOException | TemplateException e) {
            log.error("Failed to generate HTML report with Freemarker", e);
            throw new RuntimeException("Failed to generate HTML report with Freemarker", e);
        }
    }
    
    /**
     * JSON工具类，用于Freemarker模板中转换对象为JSON字符串
     */
    public static class JsonHelper {
        public String toPrettyJson(Object obj) {
            if (obj == null) {
                return "null";
            }
            return JSONObject.toJSONString(obj, SerializerFeature.PrettyFormat);
        }
        
        public String toJsonWithNulls(Object obj) {
            if (obj == null) {
                return "null";
            }
            return JSONObject.toJSONString(obj, SerializerFeature.WriteMapNullValue);
        }
        
        public String toJson(Object obj) {
            if (obj == null) {
                return "null";
            }
            return JSONObject.toJSONString(obj);
        }
    }
}