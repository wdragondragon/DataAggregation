<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>${messages.getMessage("report.title")}</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .summary { background: #f5f5f5; padding: 15px; border-radius: 5px; }
        .metric { display: inline-block; margin-right: 20px; }
        .difference { border: 1px solid #ddd; margin: 10px 0; padding: 10px; }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .bilingual { border-left: 3px solid #007bff; padding-left: 10px; margin: 5px 0; }
        .chinese { font-family: 'Microsoft YaHei', sans-serif; }
        .resolved-row { background-color: #e8f5e8; padding: 10px; margin: 10px 0; border-left: 4px solid #4CAF50; }
        .field-diff { background-color: #fff3cd; padding: 5px; margin: 2px 0; border-left: 3px solid #ffc107; }
        .update-stats { background-color: #e8f4fd; padding: 15px; border-radius: 5px; margin: 20px 0; border-left: 4px solid #2196F3; }
        .update-success { color: #4CAF50; }
        .update-failure { color: #f44336; }
        .update-failure-item { background-color: #ffebee; padding: 10px; margin: 5px 0; border-left: 3px solid #f44336; }
    </style>
</head>
<body<#if language == "CHINESE"> class="chinese"</#if>>
    <h1>${messages.getMessage("report.title")}</h1>
    
    <div class="summary">
        <h2>${messages.getMessage("report.summary")}</h2>
        <div class="metric"><strong>${messages.getMessage("metric.rule.id")}:</strong> ${result.ruleId}</div>
        <div class="metric"><strong>${messages.getMessage("metric.execution.time")}:</strong> ${dateFormat.format(result.executionTime)}</div>
        <div class="metric"><strong>${messages.getMessage("metric.status")}:</strong> ${result.status}</div>
        <div class="metric"><strong>${messages.getMessage("metric.total.records")}:</strong> ${result.totalRecords}</div>
        <div class="metric"><strong>${messages.getMessage("metric.consistent.records")}:</strong> ${result.consistentRecords}</div>
        <div class="metric"><strong>${messages.getMessage("metric.inconsistent.records")}:</strong> ${result.inconsistentRecords}</div>
        <div class="metric"><strong>${messages.getMessage("metric.resolved.records")}:</strong> ${result.resolvedRecords}</div>
    </div>

    <#-- Update results section -->
    <#if result.updateResult??>
        <div class="update-stats">
            <h2>${messages.getMessage("report.update.results")}</h2>
            <p><strong>${messages.getMessage("update.target.source")}:</strong> ${result.updateResult.targetSourceId}</p>
            <p><strong>${messages.getMessage("update.total.updates")}:</strong> ${result.updateResult.totalUpdates}</p>
            <p><strong class="update-success">${messages.getMessage("update.successful")}:</strong> ${result.updateResult.successfulUpdates}</p>
            <p><strong class="update-failure">${messages.getMessage("update.failed")}:</strong> ${result.updateResult.failedUpdates}</p>

            <#if result.updateResult.failures?? && result.updateResult.failures?size gt 0>
                <h3>${messages.getMessage("update.failure.details")}</h3>
                <#list result.updateResult.failures as failure>
                    <div class="update-failure-item">
                        <p><strong>${messages.getMessage("difference.record.id")}:</strong> ${failure.recordId}</p>
                        <#if failure.matchKeys?? && failure.matchKeys?size gt 0>
                            <p><strong>${messages.getMessage("difference.match.keys")}:</strong> ${jsonHelper.toJson(failure.matchKeys)}</p>
                        </#if>
                        <p><strong>${messages.getMessage("update.failure.reason")}:</strong> ${failure.reason}</p>
                    </div>
                </#list>
            </#if>
        </div>
    </#if>
<#--    <#if result.resolvedRows?? && result.resolvedRows?size gt 0>-->
<#--        <h2>${messages.getMessage("report.resolved.rows")} (${result.resolvedRows?size})</h2>-->
<#--        <#list result.resolvedRows as row>-->
<#--            <div class="resolved-row">-->
<#--                <h3>${messages.getMessage("difference.header", row_index + 1)}</h3>-->
<#--                 <pre>${jsonHelper.toPrettyJson(row)}</pre>-->
<#--            </div>-->
<#--        </#list>-->
<#--    </#if>-->
    
    <#if differences?size gt 0>
        <h2>${messages.getMessage("report.differences.found", differences?size)}</h2>
        
        <#list differences as diff>
            <#if diff_index lt 100>
                <div class="difference">
                    <h3>${messages.getMessage("difference.header", diff_index + 1)}</h3>
                    <p><strong>${messages.getMessage("difference.record.id")}:</strong> ${diff.recordId}</p>
                     <p><strong>${messages.getMessage("difference.match.keys")}:</strong> ${jsonHelper.toJson(diff.matchKeyValues)}</p>
                    <p><strong>${messages.getMessage("difference.conflict.type")}:</strong> ${messages.getConflictType(diff.conflictType)}</p>
                    <p><strong>${messages.getMessage("difference.discrepancy.score")}:</strong> ${diff.discrepancyScore?string("0.00")}</p>
                    
                    <h4>${messages.getMessage("difference.differences.field")}</h4>
                    <#if diff.differences??>
                        <#list diff.differences?keys as field>
                            <div class="field-diff">
                                <strong>${field}:</strong> ${diff.differences[field]}
                            </div>
                        </#list>
                    </#if>
                    
                    <#if diff.resolutionResult??>
                        <h4>${messages.getMessage("difference.resolution.result")}</h4>
                        <p><strong>${messages.getMessage("difference.strategy.used")}:</strong> ${messages.getStrategy(diff.resolutionResult.strategyUsed)}</p>
                        <#if diff.resolutionResult.winningSource??>
                            <p><strong>${messages.getMessage("difference.winning.source")}:</strong> ${diff.resolutionResult.winningSource}</p>
                        </#if>
                        <#if diff.resolutionResult.resolvedValues??>
                            <p><strong>${messages.getMessage("difference.result.values")}:</strong></p>
                             <pre>${jsonHelper.toJsonWithNulls(diff.resolutionResult.resolvedValues)}</pre>
                        </#if>
                        <p><strong>${messages.getMessage("difference.resolution.time")}:</strong> ${dateFormat.format(diff.resolutionResult.resolutionTime)}</p>
                    </#if>
                </div>
            </#if>
        </#list>
        
        <#if differences?size gt 100>
            <p>${messages.getMessage("report.more.differences", differences?size - 100)}</p>
        </#if>
    <#else>
        <h2>${messages.getMessage("report.no.differences")}</h2>
        <p>${messages.getMessage("report.all.consistent")}</p>
    </#if>
</body>
</html>