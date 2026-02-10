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
        .operation-insert { border-left: 4px solid #4CAF50; }
        .operation-update { border-left: 4px solid #FFC107; }
        .operation-delete { border-left: 4px solid #F44336; }
        .operation-insert caption { background-color: #e8f5e8; }
        .operation-update caption { background-color: #fff3cd; }
        .operation-delete caption { background-color: #ffebee; }
        .operation-text-insert { color: #4CAF50; font-weight: bold; font-size: 1.1em; text-transform: uppercase; text-shadow: 0 1px 2px rgba(0,0,0,0.1); }
        .operation-text-update { color: #FFC107; font-weight: bold; font-size: 1.1em; text-transform: uppercase; text-shadow: 0 1px 2px rgba(0,0,0,0.1); }
        .operation-text-delete { color: #F44336; font-weight: bold; font-size: 1.1em; text-transform: uppercase; text-shadow: 0 1px 2px rgba(0,0,0,0.1); }
        table { border-collapse: collapse; width: 100%; }
        th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
        th { background-color: #f2f2f2; }
        .bilingual { border-left: 3px solid #007bff; padding-left: 10px; margin: 5px 0; }
        .chinese { font-family: 'Microsoft YaHei', sans-serif; }

        .field-diff { background-color: #fff3cd; padding: 5px; margin: 2px 0; border-left: 3px solid #ffc107; }
        .update-stats { background-color: #e8f4fd; padding: 15px; border-radius: 5px; margin: 20px 0; border-left: 4px solid #2196F3; }
        .update-success { color: #4CAF50; }
        .update-failure { color: #f44336; }
        .update-failure-item { background-color: #ffebee; padding: 10px; margin: 5px 0; border-left: 3px solid #f44336; }
        .resolution-row { background-color: #e8f5e8; font-weight: bold; }
        .match-key-field { background-color: #e8f5e8; }
        .match-key-header { background-color: #e8f5e8; }
        .field-with-diff { background-color: #fff3cd; font-weight: bold; }
        .field-with-diff-header { background-color: #ffeb3b; color: #000; }
        .resolution-info-row { background-color: #f5f5f5; font-size: 0.9em; color: #666; }
        .missing-source-row { background-color: #ffebee; }
        .missing-source-cell { color: #d32f2f; font-style: italic; }
    </style>
</head>
<body<#if language == "CHINESE"> class="chinese"</#if>>
    <#assign maxDiffDisplay = (outputConfig.maxDifferencesToDisplay)!100>
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
        <div class="metric"><strong>${messages.getMessage("metric.consistency.rate")}:</strong> <#if result.totalRecords gt 0>${(result.consistentRecords / result.totalRecords * 100)?string("0.00")}%<#else>N/A</#if></div>
        
        <#-- Add update operation statistics if updates were executed -->
        <#if result.updateResult??>
            <#assign updateResult = result.updateResult>
            <div class="metric"><strong>${messages.getMessage("update.target.source")}:</strong> ${updateResult.targetSourceId}</div>
            <div class="metric"><strong>${messages.getMessage("update.total.updates")}:</strong> ${updateResult.totalUpdates}</div>
            <div class="metric"><strong class="update-success">${messages.getMessage("update.successful")}:</strong> ${updateResult.successfulUpdates}</div>
            <div class="metric"><strong class="update-failure">${messages.getMessage("update.failed")}:</strong> ${updateResult.failedUpdates}</div>
            <div class="metric"><strong>${messages.getMessage("update.insert.count")}:</strong> ${updateResult.insertCount}</div>
            <div class="metric"><strong>${messages.getMessage("update.update.count")}:</strong> ${updateResult.updateCount}</div>
            <div class="metric"><strong>${messages.getMessage("update.delete.count")}:</strong> ${updateResult.deleteCount}</div>
            <div class="metric"><strong>${messages.getMessage("update.skip.count")}:</strong> ${updateResult.skipCount}</div>
        </#if>
    </div>

    <#-- Update failure details section (only shown if there are failures) -->
    <#if result.updateResult?? && result.updateResult.failures?? && result.updateResult.failures?size gt 0>
        <div class="update-stats">
            <h2>${messages.getMessage("update.failure.details")} (${result.updateResult.failedUpdates})</h2>
            <#list result.updateResult.failures as failure>
                <div class="update-failure-item">
                    <p><strong>${messages.getMessage("difference.record.id")}:</strong> ${failure.recordId}</p>
                    <#if failure.matchKeys?? && failure.matchKeys?size gt 0>
                        <p><strong>${messages.getMessage("difference.match.keys")}:</strong> ${jsonHelper.toJson(failure.matchKeys)}</p>
                    </#if>
                    <p><strong>${messages.getMessage("update.failure.reason")}:</strong> ${failure.reason}</p>
                </div>
            </#list>
        </div>
    </#if>
    

    <#if differences?size gt 0>
        <h2>${messages.getMessage("report.differences.found", differences?size)}</h2>
        
        <#list differences as diff>
            <#if diff_index lt maxDiffDisplay>
                <#-- 获取操作类型（如果可用） -->
                <#assign operationType = "">
                <#if result.updateResult?? && result.updateResult.operationTypes??>
                    <#assign operationType = result.updateResult.operationTypes[diff.recordId]!''>
                </#if>
                <div class="difference<#if operationType?has_content> operation-${operationType?lower_case}</#if>">
                    
                    <#-- 构建字段列表：先match keys，然后对比字段 -->
                    <#assign allFields = []>
                    
                    <#-- 从metadata获取matchKeys和compareFields，如果不可用则使用回退逻辑 -->
                    <#assign ruleMatchKeys = result.metadata.matchKeys![]>
                    <#assign ruleCompareFields = result.metadata.compareFields![]>
                    
                    <#-- 添加匹配键 -->
                    <#if ruleMatchKeys?size gt 0>
                        <#-- 使用规则中定义的匹配键顺序 -->
                        <#list ruleMatchKeys as key>
                            <#if diff.matchKeyValues?? && diff.matchKeyValues[key]??>
                                <#if !allFields?seq_contains(key)>
                                    <#assign allFields = allFields + [key]>
                                </#if>
                            </#if>
                        </#list>
                    <#elseif diff.matchKeyValues??>
                        <#-- 回退：使用diff中的匹配键 -->
                        <#list diff.matchKeyValues?keys as key>
                            <#if !allFields?seq_contains(key)>
                                <#assign allFields = allFields + [key]>
                            </#if>
                        </#list>
                    </#if>
                    
                    <#-- 添加对比字段 -->
                    <#if ruleCompareFields?size gt 0>
                        <#-- 使用规则中定义的对比字段 -->
                        <#list ruleCompareFields as field>
                            <#if !allFields?seq_contains(field)>
                                <#assign allFields = allFields + [field]>
                            </#if>
                        </#list>
                    <#elseif diff.differences?? && diff.differences?size gt 0>
                        <#-- 回退：只添加有差异的字段 -->
                        <#list diff.differences?keys as field>
                            <#if !allFields?seq_contains(field)>
                                <#assign allFields = allFields + [field]>
                            </#if>
                        </#list>
                    </#if>
                    
                    <#if diff.sourceValues?? && diff.sourceValues?size gt 0>
                        <table class="data-table">
                            <caption style="text-align: left; padding: 8px; background-color: #f5f5f5; border: 1px solid #ddd; border-bottom: none;">
                                <strong>${messages.getMessage("difference.header", diff_index + 1)}</strong> | 
                                <strong>${messages.getMessage("difference.record.id")}:</strong> ${diff.recordId} | 
                                <strong>${messages.getMessage("difference.conflict.type")}:</strong> ${messages.getConflictType(diff.conflictType)} | 
                                <strong>${messages.getMessage("difference.discrepancy.score")}:</strong> ${diff.discrepancyScore?string("0.00")}<#if operationType?has_content> | 
                                <strong>${messages.getMessage("difference.operation.type")}:</strong> <span class="operation-text operation-text-${operationType?lower_case}">${operationType}</span></#if>
                            </caption>
                            <thead>
                                <tr>
                                    <th>${messages.getMessage("difference.data.source")}</th>
                                     <#list allFields as field>
                                         <#assign isMatchKey = diff.matchKeyValues?? && diff.matchKeyValues?keys?seq_contains(field)>
                                         <#assign hasDiff = diff.differences?? && diff.differences?keys?seq_contains(field)>
                                         <th<#if isMatchKey> class="match-key-header"<#elseif hasDiff> class="field-with-diff-header"</#if>>${field}</th>
                                     </#list>
                                </tr>
                            </thead>
                            <tbody>
                                 <#list diff.sourceValues?keys as sourceId>
                                    <#assign isMissingSource = diff.missingSources?? && diff.missingSources?seq_contains(sourceId)>
                                    <tr<#if isMissingSource> class="missing-source-row" title="Data missing from this source"</#if>>
                                        <td<#if isMissingSource> class="missing-source-cell"</#if>>${sourceId}<#if isMissingSource> (missing)</#if></td>
                                         <#list allFields as field>
                                            <#assign isMatchKey = diff.matchKeyValues?? && diff.matchKeyValues?keys?seq_contains(field)>
                                            <#assign hasDiff = diff.differences?? && diff.differences?keys?seq_contains(field)>
                                            <td<#if isMatchKey> class="match-key-field"<#elseif hasDiff> class="field-with-diff"</#if>>
                                                 <#if diff.sourceValues[sourceId]?? && diff.sourceValues[sourceId][field]??>
                                                     ${diff.sourceValues[sourceId][field]?string}
                                                 <#else>
                                                     null
                                                 </#if>
                                            </td>
                                        </#list>
                                    </tr>
                                </#list>
                                
                                <#-- 采纳结果行 -->
                                <#if diff.resolutionResult?? && diff.resolutionResult.resolvedValues??>
                                    <tr class="resolution-row">
                                        <td><strong>${messages.getMessage("difference.result.values")}</strong></td>
                                         <#list allFields as field>
                                            <#assign isMatchKey = diff.matchKeyValues?? && diff.matchKeyValues?keys?seq_contains(field)>
                                            <#assign hasDiff = diff.differences?? && diff.differences?keys?seq_contains(field)>
                                            <td<#if isMatchKey> class="match-key-field"<#elseif hasDiff> class="field-with-diff"</#if>>
                                                 <#if diff.resolutionResult.resolvedValues[field]??>
                                                     <strong>${diff.resolutionResult.resolvedValues[field]?string}</strong>
                                                 <#else>
                                                     null
                                                 </#if>
                                            </td>
                                        </#list>
                                    </tr>
                                    
                                    <#-- 解决结果详细信息行 -->
                                    <tr class="resolution-info-row">
                                        <td colspan="${allFields?size + 1}">
                                            <small>
                                                <strong>${messages.getMessage("difference.strategy.used")}:</strong> ${messages.getStrategy(diff.resolutionResult.strategyUsed)}
                                                <#if diff.resolutionResult.winningSource??>
                                                    | <strong>${messages.getMessage("difference.winning.source")}:</strong> ${diff.resolutionResult.winningSource}
                                                </#if>
                                                | <strong>${messages.getMessage("difference.resolution.time")}:</strong> ${dateFormat.format(diff.resolutionResult.resolutionTime)}
                                            </small>
                                        </td>
                                    </tr>
                                </#if>
                            </tbody>
                        </table>
                    </#if>
                </div>
            </#if>
        </#list>
        
        <#if differences?size gt maxDiffDisplay>
            <p>${messages.getMessage("report.more.differences", differences?size - maxDiffDisplay)}</p>
        </#if>
    <#else>
        <h2>${messages.getMessage("report.no.differences")}</h2>
        <p>${messages.getMessage("report.all.consistent")}</p>
    </#if>
</body>
</html>