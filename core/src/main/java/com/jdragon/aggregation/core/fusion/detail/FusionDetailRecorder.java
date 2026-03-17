package com.jdragon.aggregation.core.fusion.detail;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.jdragon.aggregation.core.fusion.config.*;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 融合详情记录器
 */
@Slf4j
public class FusionDetailRecorder {
    
    private final FusionDetailConfig config;
    private final FusionConfig fusionConfig;
    private final Random random = new Random();
    
    // 数据存储
    private final List<FusionDetail> fusionDetails = new CopyOnWriteArrayList<>();
    private final FusionDetailOutput output;
    private final long startTime;
    
    // 作业信息
    private String jobId;
    
    /**
     * 构造函数
     */
    public FusionDetailRecorder(FusionConfig fusionConfig) {
        this.fusionConfig = fusionConfig;
        this.config = fusionConfig.getDetailConfig();
        this.startTime = System.currentTimeMillis();
        this.output = FusionDetailOutput.createDefault();
        
        // 初始化元数据
        initMetadata();
    }
    
    /**
     * 初始化元数据
     */
    private void initMetadata() {
        FusionDetailOutput.Metadata metadata = output.getMetadata();
        metadata.setFusionJobId(generateJobId());
        metadata.setConfig(fusionConfig);
        metadata.setDataSources(fusionConfig.getSources());
        metadata.setDetailConfig(config);
        
        // 构建简化字段映射
        List<Map<String, Object>> fieldMappings = new ArrayList<>();
        if (fusionConfig.getFieldMappings() != null) {
            for (FieldMapping mapping : fusionConfig.getFieldMappings()) {
                Map<String, Object> map = new HashMap<>();
                map.put("targetField", mapping.getTargetField());
                map.put("mappingType", mapping.getMappingType().name());
                // 根据不同类型添加不同信息
                FieldMapping.MappingType mappingType = mapping.getMappingType();
                switch (mappingType) {
                    case DIRECT:
                        if (mapping instanceof DirectFieldMapping) {
                            map.put("sourceField", ((DirectFieldMapping) mapping).getSourceField());
                        }
                        break;
                    case EXPRESSION:
                        if (mapping instanceof ExpressionFieldMapping) {
                            map.put("expression", ((ExpressionFieldMapping) mapping).getExpression());
                        }
                        break;
                    case CONDITIONAL:
                        if (mapping instanceof ConditionalFieldMapping) {
                            map.put("condition", ((ConditionalFieldMapping) mapping).getCondition());
                            map.put("trueValue", ((ConditionalFieldMapping) mapping).getTrueValue());
                            map.put("falseValue", ((ConditionalFieldMapping) mapping).getFalseValue());
                        }
                        break;
                    case GROOVY:
                        if (mapping instanceof GroovyFieldMapping) {
                            map.put("groovyScript", ((GroovyFieldMapping) mapping).getScript());
                        }
                        break;
                    case CONSTANT:
                        if (mapping instanceof ConstantFieldMapping) {
                            map.put("constantValue", ((ConstantFieldMapping) mapping).getConstantValue());
                        }
                        break;
                }
                fieldMappings.add(map);
            }
        }
        metadata.setFieldMappings(fieldMappings);
        
        this.jobId = metadata.getFusionJobId();
    }
    
    /**
     * 生成作业ID
     */
    private String generateJobId() {
        return "fusion_" + System.currentTimeMillis() + "_" + random.nextInt(1000);
    }
    
    /**
     * 判断是否应该记录该行详情（采样判断）
     */
    public boolean shouldRecord() {
        if (!config.isEnabled()) {
            return false;
        }
        
        // 采样率判断
        if (config.getSamplingRate() >= 1.0) {
            return true; // 100%采样
        }
        
        return random.nextDouble() < config.getSamplingRate();
    }
    
    /**
     * 记录单行融合详情
     */
    public void recordDetail(FusionDetail detail) {
        if (!config.isEnabled()) {
            return;
        }
        
        // 检查最大记录数限制
        if (fusionDetails.size() >= config.getMaxRecords()) {
            // FIFO淘汰：移除最早记录
            synchronized (fusionDetails) {
                if (fusionDetails.size() >= config.getMaxRecords()) {
                    fusionDetails.remove(0);
                }
            }
        }
        
        fusionDetails.add(detail);
        
        // 更新统计摘要
        output.getSummary().update(detail);
    }
    
    /**
     * 获取当前记录数
     */
    public int getRecordCount() {
        return fusionDetails.size();
    }
    
    /**
     * 保存详情到文件
     */
    public String saveToFile() {
        if (!config.isEnabled()) {
            log.info("融合详情记录未启用，跳过保存");
            return null;
        }
        
        try {
            // 更新统计信息
            updateSummary();
            
            // 设置输出数据
            output.setFusionDetails(new ArrayList<>(fusionDetails));
            
            // 获取文件路径
            String filePath = getOutputFilePath();
            
            // 确保目录存在
            ensureDirectoryExists(filePath);
            
            // 转换为JSON并保存
            String json = JSON.toJSONString(output, SerializerFeature.PrettyFormat,
                    SerializerFeature.WriteMapNullValue, SerializerFeature.WriteDateUseDateFormat);
            
            Path path = Paths.get(filePath);
            Files.write(path, json.getBytes());
            
            log.info("融合详情已保存到文件: {}", filePath);
            
            // 同时生成前端HTML文件
            generateHtmlFile(filePath);
            
            return filePath;
            
        } catch (Exception e) {
            log.error("保存融合详情失败", e);
            return null;
        }
    }
    
    /**
     * 获取输出文件路径
     */
    private String getOutputFilePath() {
        String savePath = config.getSavePath();
        if (savePath == null || savePath.trim().isEmpty()) {
            // 默认文件名
            return "fusion_details_" + System.currentTimeMillis() + ".json";
        }
        
        // 如果是目录，生成文件名
        if (config.isSavePathDirectory()) {
            String fileName = config.getFileNamePattern();
            
            // 替换占位符
            fileName = fileName.replace("{timestamp}", String.valueOf(System.currentTimeMillis()));
            fileName = fileName.replace("{jobId}", jobId);
            fileName = fileName.replace("{datetime}", new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date()));
            fileName = fileName.replace("{sourceCount}", String.valueOf(fusionConfig.getSources().size()));
            fileName = fileName.replace("{recordCount}", String.valueOf(fusionDetails.size()));
            
            // 确保文件名以.json结尾
            if (!fileName.endsWith(".json")) {
                fileName += ".json";
            }
            
            return savePath + File.separator + fileName;
        }
        
        // 已经是完整文件路径
        return savePath;
    }
    
    /**
     * 确保目录存在
     */
    private void ensureDirectoryExists(String filePath) throws IOException {
        File file = new File(filePath);
        File parentDir = file.getParentFile();
        if (parentDir != null && !parentDir.exists()) {
            Files.createDirectories(parentDir.toPath());
        }
    }
    
    /**
     * 更新统计摘要
     */
    private void updateSummary() {
        FusionDetailOutput.Summary summary = output.getSummary();
        summary.setTotalRecords(output.getSummary().getSampledRecords()); // 注意：这里总记录数=采样记录数，实际应该从上下文获取
        summary.setProcessingTimeMs(System.currentTimeMillis() - startTime);
        summary.setSamplingRate(config.getSamplingRate());
    }
    
    /**
     * 生成HTML可视化文件
     */
    private void generateHtmlFile(String jsonFilePath) throws IOException {
        String htmlContent = buildHtmlContent(jsonFilePath);
        String htmlFilePath = jsonFilePath.replace(".json", ".html");
        
        Path path = Paths.get(htmlFilePath);
        Files.write(path, htmlContent.getBytes());
        
        log.info("融合详情可视化页面已生成: {}", htmlFilePath);
    }
    
    /**
     * 构建HTML内容
     */
    private String buildHtmlContent(String jsonFilePath) {
        // 返回通用HTML模板，不嵌入任何数据
        return getHtmlTemplate();
    }
    
    /**
     * 获取HTML模板
     */
    private String getHtmlTemplate() {
        return "<!DOCTYPE html>\n" +
                "<html lang=\"zh-CN\">\n" +
                "<head>\n" +
                "    <meta charset=\"UTF-8\">\n" +
                "    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">\n" +
                "    <title>数据融合详情可视化 - ${JOB_ID}</title>\n" +
                "    <link href=\"https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css\" rel=\"stylesheet\">\n" +
                "    <link href=\"https://cdn.jsdelivr.net/npm/bootstrap-icons@1.8.1/font/bootstrap-icons.css\" rel=\"stylesheet\">\n" +
                "    <script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>\n" +
                "    <script src=\"https://cdn.jsdelivr.net/npm/vue@3/dist/vue.global.prod.js\"></script>\n" +
                "    <style>\n" +
                "        body { background-color: #f8f9fa; }\n" +
                "        .card { margin-bottom: 1rem; }\n" +
                "        .table-hover tbody tr:hover { background-color: rgba(0,0,0,.075); }\n" +
                "        .badge-success { background-color: #28a745; }\n" +
                "        .badge-error { background-color: #dc3545; }\n" +
                "        .badge-skipped { background-color: #6c757d; }\n" +
                "        .field-detail { background-color: #f8f9fa; border-left: 4px solid #007bff; }\n" +
                "        .source-value { font-family: monospace; background-color: #e9ecef; padding: 2px 6px; border-radius: 3px; }\n" +
                "        .chart-container { position: relative; height: 300px; }\n" +
                "    </style>\n" +
                "</head>\n" +
                "<body>\n" +
                "    <div id=\"app\" class=\"container-fluid py-3\">\n" +
                "        <!-- 标题栏 -->\n" +
                "        <div class=\"row mb-3\">\n" +
                "            <div class=\"col\">\n" +
                "                <h1 class=\"h3\">数据融合详情可视化</h1>\n" +
"                <div class=\"text-muted small\">\n" +
"                    作业ID: <span id=\"jobId\">--</span> | 记录数: <span id=\"recordCount\">0</span> | 生成时间: <span id=\"timestamp\">--</span>\n" +
"                </div>\n" +
"                <div class=\"mt-2\">\n" +
"                    <small class=\"text-info\" id=\"dataStatus\">请加载JSON文件以查看融合详情</small>\n" +
"                </div>\n" +
                "            </div>\n" +
"            <div class=\"col-auto\">\n" +
"                <input type=\"file\" id=\"jsonFileInput\" accept=\".json\" style=\"display: none;\" @change=\"handleFileSelect\">\n" +
"                <button class=\"btn btn-outline-primary\" @click=\"openFilePicker\">\n" +
"                    <i class=\"bi-folder2-open\"></i> 打开JSON文件\n" +
"                </button>\n" +
"                <button class=\"btn btn-outline-secondary ms-2\" @click=\"reloadData\" :disabled=\"!currentFile\">\n" +
"                    <i class=\"bi-arrow-clockwise\"></i> 重新加载\n" +
"                </button>\n" +
"            </div>\n" +
                "        </div>\n" +
                "\n" +
                "        <!-- 概览仪表板 -->\n" +
                "        <div class=\"row mb-3\">\n" +
                "            <div class=\"col-md-3\">\n" +
                "                <div class=\"card text-white bg-primary\">\n" +
                "                    <div class=\"card-body\">\n" +
                "                        <h5 class=\"card-title\">总记录数</h5>\n" +
                "                        <p class=\"card-text display-6\">{{ summary.totalRecords || 0 }}</p>\n" +
                "                    </div>\n" +
                "                </div>\n" +
                "            </div>\n" +
                "            <div class=\"col-md-3\">\n" +
                "                <div class=\"card text-white bg-success\">\n" +
                "                    <div class=\"card-body\">\n" +
                "                        <h5 class=\"card-title\">成功记录</h5>\n" +
                "                        <p class=\"card-text display-6\">{{ summary.successfulRecords || 0 }}</p>\n" +
                "                        <small>成功率: {{ (summary.successRate || 0).toFixed(2) }}%</small>\n" +
                "                    </div>\n" +
                "                </div>\n" +
                "            </div>\n" +
                "            <div class=\"col-md-3\">\n" +
                "                <div class=\"card text-white bg-danger\">\n" +
                "                    <div class=\"card-body\">\n" +
                "                        <h5 class=\"card-title\">错误记录</h5>\n" +
                "                        <p class=\"card-text display-6\">{{ summary.errorRecords || 0 }}</p>\n" +
                "                        <small>错误率: {{ (summary.errorRate || 0).toFixed(2) }}%</small>\n" +
                "                    </div>\n" +
                "                </div>\n" +
                "            </div>\n" +
                "            <div class=\"col-md-3\">\n" +
                "                <div class=\"card text-white bg-info\">\n" +
                "                    <div class=\"card-body\">\n" +
                "                        <h5 class=\"card-title\">处理时间</h5>\n" +
                "                        <p class=\"card-text display-6\">{{ (summary.processingTimeMs / 1000).toFixed(2) }}s</p>\n" +
                "                        <small>采样率: {{ (summary.samplingRate * 100).toFixed(1) }}%</small>\n" +
                "                    </div>\n" +
                "                </div>\n" +
                "            </div>\n" +
                "        </div>\n" +
                "\n" +
                "        <!-- 图表区域 -->\n" +
                "        <div class=\"row mb-3\">\n" +
                "            <div class=\"col-md-6\">\n" +
                "                <div class=\"card\">\n" +
                "                    <div class=\"card-header\">\n" +
                "                        <h5 class=\"card-title mb-0\">错误分布</h5>\n" +
                "                    </div>\n" +
                "                    <div class=\"card-body\">\n" +
                "                        <div class=\"chart-container\">\n" +
                "                            <canvas id=\"errorChart\"></canvas>\n" +
                "                        </div>\n" +
                "                    </div>\n" +
                "                </div>\n" +
                "            </div>\n" +
                "            <div class=\"col-md-6\">\n" +
                "                <div class=\"card\">\n" +
                "                    <div class=\"card-header\">\n" +
                "                        <h5 class=\"card-title mb-0\">策略使用情况</h5>\n" +
                "                    </div>\n" +
                "                    <div class=\"card-body\">\n" +
                "                        <div class=\"chart-container\">\n" +
                "                            <canvas id=\"strategyChart\"></canvas>\n" +
                "                        </div>\n" +
                "                    </div>\n" +
                "                </div>\n" +
                "            </div>\n" +
                "        </div>\n" +
                "\n" +
                "        <!-- 筛选面板 -->\n" +
                "        <div class=\"row mb-3\">\n" +
                "            <div class=\"col\">\n" +
                "                <div class=\"card\">\n" +
                "                    <div class=\"card-body\">\n" +
                "                        <div class=\"row g-3\">\n" +
                "                            <div class=\"col-md-3\">\n" +
                "                                <label class=\"form-label\">状态筛选</label>\n" +
                "                                <select class=\"form-select\" v-model=\"filters.status\">\n" +
                "                                    <option value=\"\">全部状态</option>\n" +
                "                                    <option value=\"SUCCESS\">成功</option>\n" +
                "                                    <option value=\"ERROR\">错误</option>\n" +
                "                                    <option value=\"SKIPPED\">跳过</option>\n" +
                "                                </select>\n" +
                "                            </div>\n" +
                "                            <div class=\"col-md-3\">\n" +
                "                                <label class=\"form-label\">数据源筛选</label>\n" +
                "                                <select class=\"form-select\" v-model=\"filters.sourceId\">\n" +
                "                                    <option value=\"\">全部数据源</option>\n" +
                "                                    <option v-for=\"source in metadata.dataSources\" :value=\"source.sourceId\">\n" +
                "                                        {{ source.sourceId }} ({{ source.name || source.sourceId }})\n" +
                "                                    </option>\n" +
                "                                </select>\n" +
                "                            </div>\n" +
                "                            <div class=\"col-md-3\">\n" +
                "                                <label class=\"form-label\">字段搜索</label>\n" +
                "                                <input type=\"text\" class=\"form-control\" v-model=\"filters.fieldSearch\" placeholder=\"输入字段名...\">\n" +
                "                            </div>\n" +
                "                            <div class=\"col-md-3\">\n" +
                "                                <label class=\"form-label\">连接键搜索</label>\n" +
                "                                <input type=\"text\" class=\"form-control\" v-model=\"filters.joinKeySearch\" placeholder=\"输入连接键...\">\n" +
                "                            </div>\n" +
                "                        </div>\n" +
                "                    </div>\n" +
                "                </div>\n" +
                "            </div>\n" +
                "        </div>\n" +
                "\n" +
                "        <!-- 详情表格 -->\n" +
                "        <div class=\"row\">\n" +
                "            <div class=\"col\">\n" +
                "                <div class=\"card\">\n" +
                "                    <div class=\"card-header d-flex justify-content-between align-items-center\">\n" +
                "                        <h5 class=\"card-title mb-0\">融合详情</h5>\n" +
                "                        <div>\n" +
                "                            <span class=\"badge bg-secondary me-2\">\n" +
                "                                显示 {{ filteredDetails.length }} / {{ fusionDetails.length }} 条记录\n" +
                "                            </span>\n" +
                "                            <button class=\"btn btn-sm btn-outline-secondary\" @click=\"exportData\">\n" +
                "                                <i class=\"bi-download\"></i> 导出CSV\n" +
                "                            </button>\n" +
                "                        </div>\n" +
                "                    </div>\n" +
                "                    <div class=\"card-body p-0\">\n" +
                "                        <div class=\"table-responsive\" style=\"max-height: 600px; overflow-y: auto;\">\n" +
                "                            <table class=\"table table-hover table-striped mb-0\">\n" +
                "                                <thead class=\"table-light sticky-top\">\n" +
                "                                    <tr>\n" +
                "                                        <th width=\"50\">#</th>\n" +
                "                                        <th width=\"150\">连接键</th>\n" +
                "                                        <th width=\"120\">状态</th>\n" +
                "                                        <th width=\"100\">字段数</th>\n" +
                "                                        <th>字段详情</th>\n" +
                                        "                                        <th width=\"120\">操作</th>\n" +
                "                                    </tr>\n" +
                "                                </thead>\n" +
                "                                <tbody>\n" +
                "                                    <template v-for=\"(detail, index) in paginatedDetails\" :key=\"detail.joinKey\">\n" +
                "                                        <tr>\n" +
                "                                            <td>{{ (currentPage - 1) * pageSize + index + 1 }}</td>\n" +
                "                                            <td>\n" +
                "                                                <code>{{ detail.joinKey }}</code>\n" +
                "                                            </td>\n" +
                "                                            <td>\n" +
                "                                                <span class=\"badge\" :class=\"getStatusBadgeClass(detail.status)\">\n" +
                "                                                    {{ detail.status || 'UNKNOWN' }}\n" +
                "                                                </span>\n" +
                "                                            </td>\n" +
                "                                            <td>{{ detail.fieldDetails.length }}</td>\n" +
                "                                            <td>\n" +
                "                                                <div v-if=\"detail.fieldDetails.length > 0\">\n" +
                "                                                    <div v-for=\"field in detail.fieldDetails.slice(0, 2)\" :key=\"field.targetField\" class=\"mb-1\">\n" +
                "                                                        <small>\n" +
                "                                                            <strong>{{ field.targetField }}</strong>: \n" +
                "                                                            <span v-if=\"field.sourceRef\" class=\"text-muted\">{{ field.sourceRef }} → </span>\n" +
                "                                                            <span class=\"source-value\">{{ formatValue(field.fusedValue) }}</span>\n" +
                "                                                            <span v-if=\"field.strategyUsed\" class=\"badge bg-info ms-1\">{{ field.strategyUsed }}</span>\n" +
                "                                                        </small>\n" +
                "                                                    </div>\n" +
                "                                                    <div v-if=\"detail.fieldDetails.length > 2\">\n" +
                "                                                        <small class=\"text-muted\">... 还有 {{ detail.fieldDetails.length - 2 }} 个字段</small>\n" +
                "                                                    </div>\n" +
                "                                                </div>\n" +
                "                                                <div v-else class=\"text-muted\">\n" +
                "                                                    <small>无字段详情</small>\n" +
                "                                                </div>\n" +
                "                                            </td>\n" +
                "                                            <td>\n" +
                "                                                <button class=\"btn btn-sm btn-outline-primary\" @click=\"toggleExpand(detail)\">\n" +
                "                                                    <i class=\"bi\" :class=\"expandedDetails.has(detail.joinKey) ? 'bi-chevron-up' : 'bi-chevron-down'\"></i>\n" +
                "                                                    详情\n" +
                "                                                </button>\n" +
                "                                            </td>\n" +
                "                                        </tr>\n" +
                "                                        <!-- 展开的详情行 -->\n" +
                "                                        <tr v-if=\"expandedDetails.has(detail.joinKey)\">\n" +
                "                                            <td colspan=\"6\" class=\"p-0\">\n" +
                "                                                <div class=\"field-detail p-3 bg-light border-top\">\n" +
                "                                                    <h6 class=\"mb-3\">字段级详情</h6>\n" +
                "                                                    <div class=\"table-responsive\">\n" +
                "                                                        <table class=\"table table-sm table-bordered mb-0\">\n" +
                "                                                            <thead class=\"table-light\">\n" +
                "                                                                <tr>\n" +
                "                                                                    <th width=\"25%\">字段名</th>\n" +
                "                                                                    <th width=\"15%\">映射类型</th>\n" +
                "                                                                    <th width=\"40%\">源值/源引用</th>\n" +
                "                                                                    <th width=\"20%\">状态</th>\n" +
                "                                                                </tr>\n" +
                "                                                            </thead>\n" +
                "                                                            <tbody>\n" +
                "                                                                <tr v-for=\"field in detail.fieldDetails\" :key=\"field.targetField\">\n" +
                "                                                                    <td><strong>{{ field.targetField }}</strong></td>\n" +
                "                                                                    <td>\n" +
                "                                                                        {{ field.mappingType }}\n" +
                "                                                                        <span v-if=\"field.strategyUsed\" class=\"badge bg-info ms-1\">{{ field.strategyUsed }}</span>\n" +
                "                                                                    </td>\n" +
                "                                                                    <td>\n" +
                "                                                                        <div v-if=\"field.sourceRef\">\n" +
                "                                                                            <div><small class=\"text-muted\">源引用:</small></div>\n" +
                "                                                                            <code>{{ field.sourceRef }}</code>\n" +
                "                                                                        </div>\n" +
                "                                                                        <div v-if=\"Object.keys(field.sourceValues).length > 0\">\n" +
                "                                                                            <div><small class=\"text-muted\">源值:</small></div>\n" +
                "                                                                            <div>\n" +
                "                                                                                <span v-for=\"(value, sourceId) in field.sourceValues\" :key=\"sourceId\" class=\"me-2\">\n" +
                "                                                                                    <small>{{ sourceId }}: <span class=\"source-value\">{{ formatValue(value) }}</span></small>\n" +
                "                                                                                </span>\n" +
                "                                                                            </div>\n" +
                "                                                                        </div>\n" +
                "                                                                        <div v-else-if=\"!field.sourceRef\" class=\"text-muted\">\n" +
                "                                                                            <small>N/A</small>\n" +
                "                                                                        </div>\n" +
                "                                                                        <div class=\"mt-1\">\n" +
                "                                                                            <div><small class=\"text-muted\">融合值:</small></div>\n" +
                "                                                                            <span class=\"source-value\">{{ formatValue(field.fusedValue) }}</span>\n" +
                "                                                                        </div>\n" +
                "                                                                    </td>\n" +
                "                                                                    <td>\n" +
                "                                                                        <span class=\"badge\" :class=\"getStatusBadgeClass(field.status)\">{{ field.status }}</span>\n" +
                "                                                                        <div v-if=\"field.errorMessage\" class=\"text-danger small mt-1\">\n" +
                "                                                                            {{ field.errorMessage }}\n" +
                "                                                                        </div>\n" +
                "                                                                    </td>\n" +
                "                                                                </tr>\n" +
                "                                                            </tbody>\n" +
                "                                                        </table>\n" +
"                                                    </div>\n" +
"                                                </div>\n" +
                "                                            </td>\n" +
                "                                        </tr>\n" +
                "                                    </template>\n" +
                "                                </tbody>\n" +
                "                            </table>\n" +
                "                        </div>\n" +
                "                    </div>\n" +
                "                    <div class=\"card-footer d-flex justify-content-between align-items-center\">\n" +
                "                        <div>\n" +
                "                            <small class=\"text-muted\">\n" +
                "                                每页显示\n" +
                "                                <select class=\"form-select form-select-sm d-inline-block w-auto\" v-model=\"pageSize\">\n" +
                "                                    <option value=\"10\">10</option>\n" +
                "                                    <option value=\"25\">25</option>\n" +
                "                                    <option value=\"50\">50</option>\n" +
                "                                    <option value=\"100\">100</option>\n" +
                "                                </select>\n" +
                "                                条记录\n" +
                "                            </small>\n" +
                "                        </div>\n" +
                "                        <nav>\n" +
                "                            <ul class=\"pagination pagination-sm mb-0\">\n" +
                "                                <li class=\"page-item\" :class=\"{ disabled: currentPage === 1 }\">\n" +
                "                                    <a class=\"page-link\" href=\"#\" @click.prevent=\"currentPage = 1\">首页</a>\n" +
                "                                </li>\n" +
                "                                <li class=\"page-item\" :class=\"{ disabled: currentPage === 1 }\">\n" +
                "                                    <a class=\"page-link\" href=\"#\" @click.prevent=\"currentPage--\">上一页</a>\n" +
                "                                </li>\n" +
                "                                <li class=\"page-item disabled\">\n" +
                "                                    <span class=\"page-link\">第 {{ currentPage }} 页 / 共 {{ totalPages }} 页</span>\n" +
                                "                                </li>\n" +
                "                                <li class=\"page-item\" :class=\"{ disabled: currentPage === totalPages }\">\n" +
                "                                    <a class=\"page-link\" href=\"#\" @click.prevent=\"currentPage++\">下一页</a>\n" +
                "                                </li>\n" +
                "                                <li class=\"page-item\" :class=\"{ disabled: currentPage === totalPages }\">\n" +
                "                                    <a class=\"page-link\" href=\"#\" @click.prevent=\"currentPage = totalPages\">末页</a>\n" +
                "                                </li>\n" +
                "                            </ul>\n" +
                "                        </nav>\n" +
                "                    </div>\n" +
                "                </div>\n" +
                "            </div>\n" +
                "        </div>\n" +
                "    </div>\n" +
                "\n" +
                "    <script>\n" +
                "        const { createApp, ref, computed, onMounted, watch } = Vue;\n" +
                "\n" +
                "        createApp({\n" +
                "            setup() {\n" +
                "                // 数据状态\n" +
"                const metadata = ref({});\n" +
"                const fusionDetails = ref([]);\n" +
"                const summary = ref({});\n" +
"                const currentFile = ref(null);\n" +
"                \n" +
"                const openFilePicker = () => {\n" +
"                    document.getElementById('jsonFileInput').click();\n" +
"                };\n" +
"                \n" +
"                const handleFileSelect = (event) => {\n" +
"                    const file = event.target.files[0];\n" +
"                    if (file) {\n" +
"                        currentFile.value = file;\n" +
"                        loadData(file);\n" +
"                    }\n" +
"                };\n" +
"                \n" +
"                const reloadData = () => {\n" +
"                    if (currentFile.value) {\n" +
"                        loadData(currentFile.value);\n" +
"                    } else {\n" +
"                        const statusEl = document.getElementById('dataStatus');\n" +
"                        if (statusEl) {\n" +
"                            statusEl.textContent = '请先选择JSON文件';\n" +
"                            statusEl.className = 'text-danger';\n" +
"                        }\n" +
"                    }\n" +
"                };\n" +
"                \n" +
"                const readFileAsText = (file) => {\n" +
"                    return new Promise((resolve, reject) => {\n" +
"                        const reader = new FileReader();\n" +
"                        reader.onload = (e) => resolve(e.target.result);\n" +
"                        reader.onerror = (e) => reject(new Error('文件读取失败'));\n" +
"                        reader.readAsText(file);\n" +
"                    });\n" +
"                };\n" +
"                \n" +
"                const updatePageMetadata = (data) => {\n" +
"                    const jobIdEl = document.getElementById('jobId');\n" +
"                    const recordCountEl = document.getElementById('recordCount');\n" +
"                    const timestampEl = document.getElementById('timestamp');\n" +
"                    if (jobIdEl) jobIdEl.textContent = data.metadata?.jobId || '--';\n" +
"                    if (recordCountEl) recordCountEl.textContent = data.summary?.totalRecords || 0;\n" +
"                    if (timestampEl) timestampEl.textContent = data.metadata?.timestamp ? new Date(data.metadata.timestamp).toLocaleString() : '--';\n" +
"                };\n" +
"                \n" +
"                const updateDataStatus = (message, type = 'info') => {\n" +
"                    const statusEl = document.getElementById('dataStatus');\n" +
"                    if (statusEl) {\n" +
"                        statusEl.textContent = message;\n" +
"                        statusEl.className = type === 'success' ? 'text-success' : type === 'error' ? 'text-danger' : 'text-info';\n" +
"                    }\n" +
"                };\n" +
"                \n" +
"                // UI状态\n" +
                "                const expandedDetails = ref(new Set());\n" +
                "                const currentPage = ref(1);\n" +
                "                const pageSize = ref(25);\n" +
                "                \n" +
                "                // 筛选条件\n" +
                "                const filters = ref({\n" +
                "                    status: '',\n" +
                "                    sourceId: '',\n" +
                "                    fieldSearch: '',\n" +
                "                    joinKeySearch: ''\n" +
                "                });\n" +
                "\n" +
                "                // 计算属性\n" +
                "                const filteredDetails = computed(() => {\n" +
                "                    return fusionDetails.value.filter(detail => {\n" +
                "                        // 状态筛选\n" +
                "                        if (filters.value.status && detail.status !== filters.value.status) {\n" +
                "                            return false;\n" +
                "                        }\n" +
                "                        \n" +
                "                        // 连接键搜索\n" +
                "                        if (filters.value.joinKeySearch && \n" +
                "                            !detail.joinKey.toLowerCase().includes(filters.value.joinKeySearch.toLowerCase())) {\n" +
                "                            return false;\n" +
                "                        }\n" +
                "                        \n" +
                "                        // 字段搜索\n" +
                "                        if (filters.value.fieldSearch) {\n" +
                "                            const hasMatchingField = detail.fieldDetails.some(field => \n" +
                "                                field.targetField.toLowerCase().includes(filters.value.fieldSearch.toLowerCase())\n" +
                "                            );\n" +
                "                            if (!hasMatchingField) return false;\n" +
                "                        }\n" +
                "                        \n" +
                "                        // 数据源筛选\n" +
                "                        if (filters.value.sourceId) {\n" +
                "                            const involvesSource = detail.fieldDetails.some(field => {\n" +
                "                                if (!field.sourceRef) return false;\n" +
                "                                // 检查源引用是否包含该数据源ID\n" +
                "                                return field.sourceRef.includes(filters.value.sourceId + \".\") || \n" +
                "                                       field.sourceValues.hasOwnProperty(filters.value.sourceId);\n" +
                "                            });\n" +
                "                            if (!involvesSource) return false;\n" +
                "                        }\n" +
                "                        \n" +
                "                        return true;\n" +
                "                    });\n" +
                "                });\n" +
                "                \n" +
                "                const totalPages = computed(() => {\n" +
                "                    return Math.ceil(filteredDetails.value.length / pageSize.value);\n" +
                "                });\n" +
                "                \n" +
                "                const paginatedDetails = computed(() => {\n" +
                "                    const start = (currentPage.value - 1) * pageSize.value;\n" +
                "                    const end = start + pageSize.value;\n" +
                "                    return filteredDetails.value.slice(start, end);\n" +
                "                });\n" +
                "\n" +
                "                // 方法\n" +
"                const loadData = async (file) => {\n" +
"                    try {\n" +
"                        const fileToRead = file || currentFile.value;\n" +
"                        if (!fileToRead) {\n" +
"                            throw new Error('请先选择JSON文件');\n" +
"                        }\n" +
"                        const text = await readFileAsText(fileToRead);\n" +
"                        const data = JSON.parse(text);\n" +
"                        \n" +
"                        metadata.value = data.metadata || {};\n" +
"                        fusionDetails.value = data.fusionDetails || [];\n" +
"                        summary.value = data.summary || {};\n" +
"                        \n" +
"                        // 更新页面元数据\n" +
"                        updatePageMetadata(data);\n" +
"                        \n" +
"                        // 重置分页\n" +
"                        currentPage.value = 1;\n" +
"                        expandedDetails.value.clear();\n" +
"                        \n" +
"                        // 初始化图表\n" +
"                        setTimeout(initCharts, 100);\n" +
"                        \n" +
"                        console.log('数据加载成功:', fusionDetails.value.length, '条记录');\n" +
"                        updateDataStatus('数据加载成功', 'success');\n" +
"                    } catch (error) {\n" +
"                        console.error('加载数据失败:', error);\n" +
"                        updateDataStatus('加载数据失败: ' + error.message, 'error');\n" +
"                    }\n" +
"                };\n" +
                "                \n" +
                "                const toggleExpand = (detail) => {\n" +
                "                    if (expandedDetails.value.has(detail.joinKey)) {\n" +
                "                        expandedDetails.value.delete(detail.joinKey);\n" +
                "                    } else {\n" +
                "                        expandedDetails.value.add(detail.joinKey);\n" +
                "                    }\n" +
                "                };\n" +
                "                \n" +
                "                const getStatusBadgeClass = (status) => {\n" +
                "                    switch (status) {\n" +
                "                        case 'SUCCESS': return 'badge-success';\n" +
                "                        case 'ERROR': return 'badge-error';\n" +
                "                        case 'SKIPPED': return 'badge-skipped';\n" +
                "                        default: return 'badge-secondary';\n" +
                "                    }\n" +
                "                };\n" +
                "                \n" +
                "                const formatValue = (value) => {\n" +
                "                    if (value === null || value === undefined) return 'null';\n" +
                "                    if (typeof value === 'object') return JSON.stringify(value);\n" +
                "                    return String(value);\n" +
                "                };\n" +
                "                \n" +
                "                const exportData = () => {\n" +
                "                    // 简单的CSV导出实现\n" +
                "                    const headers = ['连接键', '状态', '字段数', '字段详情'];\n" +
                "                    const rows = filteredDetails.value.map(detail => [\n" +
                "                        detail.joinKey,\n" +
                "                        detail.status,\n" +
                "                        detail.fieldDetails.length,\n" +
                "                        detail.fieldDetails.map(f => `${f.targetField}:${f.fusedValue}`).join('; ')\n" +
                "                    ]);\n" +
                "                    \n" +
                "                    const csvContent = [\n" +
                "                        headers.join(','),\n" +
                "                        ...rows.map(row => row.map(cell => `\"${cell}\"`).join(','))\n" +
                "                    ].join('\\n');\n" +
                "                    \n" +
                "                    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });\n" +
                "                    const link = document.createElement('a');\n" +
                "                    link.href = URL.createObjectURL(blob);\n" +
                "                    link.download = `fusion_details_${new Date().getTime()}.csv`;\n" +
                "                    link.click();\n" +
                "                };\n" +
                "                \n" +
                "                const initCharts = () => {\n" +
                "                    // 错误分布图表\n" +
                "                    const errorCtx = document.getElementById('errorChart')?.getContext('2d');\n" +
                "                    if (errorCtx && fusionDetails.value.length > 0) {\n" +
                "                        const statusCounts = {};\n" +
                "                        fusionDetails.value.forEach(detail => {\n" +
                "                            statusCounts[detail.status] = (statusCounts[detail.status] || 0) + 1;\n" +
                "                        });\n" +
                "                        \n" +
                "                        new Chart(errorCtx, {\n" +
                "                            type: 'doughnut',\n" +
                "                            data: {\n" +
                "                                labels: Object.keys(statusCounts),\n" +
                "                                datasets: [{\n" +
                "                                    data: Object.values(statusCounts),\n" +
                "                                    backgroundColor: ['#28a745', '#dc3545', '#6c757d', '#ffc107']\n" +
                "                                }]\n" +
                "                            },\n" +
                "                            options: {\n" +
                "                                responsive: true,\n" +
                "                                plugins: {\n" +
                "                                    legend: { position: 'bottom' }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        });\n" +
                "                    }\n" +
                "                    \n" +
                "                    // 策略使用图表\n" +
                "                    const strategyCtx = document.getElementById('strategyChart')?.getContext('2d');\n" +
                "                    if (strategyCtx && summary.value.strategyStatistics) {\n" +
                "                        new Chart(strategyCtx, {\n" +
                "                            type: 'bar',\n" +
                "                            data: {\n" +
                "                                labels: Object.keys(summary.value.strategyStatistics),\n" +
                                "                                datasets: [{\n" +
                "                                    label: '使用次数',\n" +
                "                                    data: Object.values(summary.value.strategyStatistics),\n" +
                "                                    backgroundColor: '#007bff'\n" +
                "                                }]\n" +
                "                            },\n" +
                "                            options: {\n" +
                "                                responsive: true,\n" +
                "                                scales: {\n" +
                "                                    y: {\n" +
                "                                        beginAtZero: true,\n" +
                "                                        ticks: { stepSize: 1 }\n" +
                "                                    }\n" +
                "                                }\n" +
                "                            }\n" +
                "                        });\n" +
                "                    }\n" +
                "                };\n" +
                "\n" +
                "                // 生命周期\n" +
                "                onMounted(() => {\n" +
                "                    loadData();\n" +
                "                });\n" +
                "\n" +
                "                // 监听筛选条件变化，重置分页\n" +
                "                watch(() => filters.value, () => {\n" +
                "                    currentPage.value = 1;\n" +
                "                }, { deep: true });\n" +
                "\n" +
                "                return {\n" +
                "                    metadata,\n" +
                "                    fusionDetails,\n" +
                "                    summary,\n" +
                "                    expandedDetails,\n" +
                "                    currentPage,\n" +
                "                    pageSize,\n" +
                "                    filters,\n" +
                "                    filteredDetails,\n" +
                "                    totalPages,\n" +
                "                    paginatedDetails,\n" +
                "                    loadData,\n" +
                "                    toggleExpand,\n" +
                "                    getStatusBadgeClass,\n" +
                "                    formatValue,\n" +
"                    exportData,\n" +
"                    currentFile,\n" +
"                    openFilePicker,\n" +
"                    handleFileSelect,\n" +
"                    reloadData\n" +
"                };\n" +
                "            }\n" +
                "        }).mount('#app');\n" +
                "    </script>\n" +
                "</body>\n" +
                "</html>";
    }
}