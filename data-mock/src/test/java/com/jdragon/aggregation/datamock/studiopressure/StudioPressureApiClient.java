package com.jdragon.aggregation.datamock.studiopressure;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class StudioPressureApiClient {

    private static final ObjectMapper OBJECT_MAPPER = StudioPressureMysqlTestSupport.OBJECT_MAPPER.copy()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final StudioPressureMysqlTestSupport.PressureConfig config;
    private final String baseUrl;
    private final int timeoutMs;
    private String token;
    private String tenantId;
    private Long projectId;

    StudioPressureApiClient(StudioPressureMysqlTestSupport.PressureConfig config) {
        this.config = config;
        this.config.applyDefaults();
        this.baseUrl = trimRightSlash(this.config.getStudio().getBaseUrl());
        this.timeoutMs = this.config.getTimeouts().getRequestTimeoutMs();
    }

    void login() {
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("username", config.getStudio().getUsername());
        body.put("password", config.getStudio().getPassword());
        JsonNode data = request("POST", "/api/v1/auth/login", body, null, false);
        this.token = text(data, "token");
        this.tenantId = firstNonBlank(config.getStudio().getTenantId(), text(data, "currentTenantId"));
        this.projectId = asLong(data.get("currentProjectId"));
    }

    void useProject(Long projectId) {
        this.projectId = projectId;
    }

    Long currentProjectId() {
        return projectId;
    }

    String currentTenantId() {
        return tenantId;
    }

    ProjectRef createProject(String projectCode, String projectName) {
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("projectCode", projectCode);
        body.put("projectName", projectName);
        body.put("description", "studio pressure benchmark project");
        body.put("enabled", 1);
        body.put("defaultProject", 0);
        return convert(request("POST", "/api/v1/system/projects", body, null, true), ProjectRef.class);
    }

    List<ProjectWorkerRef> listProjectWorkers(Long projectId) {
        Map<String, String> query = new LinkedHashMap<String, String>();
        if (projectId != null) {
            query.put("projectId", String.valueOf(projectId));
        }
        return convertList(request("GET", "/api/v1/system/project-workers", null, query, true), ProjectWorkerRef.class);
    }

    ProjectWorkerBindingRef bindProjectWorker(Long projectId, String workerCode) {
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("projectId", projectId);
        body.put("workerCode", workerCode);
        body.put("enabled", 1);
        return convert(request("POST", "/api/v1/system/project-workers", body, null, true), ProjectWorkerBindingRef.class);
    }

    DataSourceRef saveMysqlDatasource(String name, String database, boolean executable) {
        return saveMysqlDatasource(name, config.getSourceMysql(), database, executable);
    }

    DataSourceRef saveMysqlDatasource(String name,
                                      StudioPressureMysqlTestSupport.MysqlConnectConfig connectConfig,
                                      String database,
                                      boolean executable) {
        Map<String, Object> technicalMetadata = new LinkedHashMap<String, Object>();
        technicalMetadata.put("host", connectConfig.getHost());
        technicalMetadata.put("port", connectConfig.getPort());
        technicalMetadata.put("database", database);
        technicalMetadata.put("userName", connectConfig.getUserName());
        technicalMetadata.put("password", connectConfig.getPassword());
        technicalMetadata.put("other", connectConfig.getOther());
        technicalMetadata.put("usePool", connectConfig.getUsePool());
        technicalMetadata.put("extraParams", connectConfig.getExtraParams());

        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("name", name);
        body.put("typeCode", "mysql8");
        body.put("enabled", true);
        body.put("executable", executable);
        body.put("technicalMetadata", technicalMetadata);
        body.put("businessMetadata", new LinkedHashMap<String, Object>());
        return convert(request("POST", "/api/v1/datasources", body, null, true), DataSourceRef.class);
    }

    DiscoveryResult discoverModels(Long datasourceId) {
        return discoverModels(datasourceId, null, null, null);
    }

    DiscoveryResult discoverModels(Long datasourceId, String keyword, Integer pageNo, Integer pageSize) {
        Map<String, String> query = new LinkedHashMap<String, String>();
        if (keyword != null && !keyword.trim().isEmpty()) {
            query.put("keyword", keyword.trim());
        }
        if (pageNo != null && pageNo.intValue() > 0) {
            query.put("pageNo", String.valueOf(pageNo));
        }
        if (pageSize != null && pageSize.intValue() > 0) {
            query.put("pageSize", String.valueOf(pageSize));
        }
        return convert(request("POST", "/api/v1/datasources/" + datasourceId + "/discover", null, query, true), DiscoveryResult.class);
    }

    List<ModelRef> syncAllModels(Long datasourceId) {
        return convertList(request("POST", "/api/v1/models/datasource/" + datasourceId + "/sync", null, null, true), ModelRef.class);
    }

    List<ModelRef> syncSelectedModels(Long datasourceId, List<String> physicalLocators) {
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("physicalLocators", physicalLocators);
        return convertList(request("POST", "/api/v1/models/datasource/" + datasourceId + "/sync-selected", body, null, true), ModelRef.class);
    }

    ModelSyncTaskRef createModelSyncTask(Long datasourceId, List<String> physicalLocators, String source) {
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("datasourceId", datasourceId);
        body.put("physicalLocators", physicalLocators);
        body.put("source", source);
        return convert(request("POST", "/api/v1/model-sync-tasks", body, null, true), ModelSyncTaskRef.class);
    }

    ModelSyncTaskRef getModelSyncTask(Long taskId) {
        return convert(request("GET", "/api/v1/model-sync-tasks/" + taskId, null, null, true), ModelSyncTaskRef.class);
    }

    List<ModelRef> listModelsByDatasource(Long datasourceId) {
        return convertList(request("GET", "/api/v1/models/datasource/" + datasourceId, null, null, true), ModelRef.class);
    }

    ModelRef saveModel(Map<String, Object> payload) {
        return convert(request("POST", "/api/v1/models", payload, null, true), ModelRef.class);
    }

    List<MetadataSchemaRef> listMetaSchemas() {
        return convertList(request("GET", "/api/v1/meta-schemas", null, null, true), MetadataSchemaRef.class);
    }

    IndexQueueStatusRef getModelIndexQueueStatus() {
        return convert(request("GET", "/api/v1/models/index/queue-status", null, null, true), IndexQueueStatusRef.class);
    }

    StatisticsOptionsRef loadStatisticsOptions(Long datasourceId, String datasourceType, String targetScope) {
        Map<String, Object> body = new LinkedHashMap<String, Object>();
        body.put("datasourceId", datasourceId);
        body.put("datasourceType", datasourceType);
        body.put("targetScope", targetScope);
        return convert(request("POST", "/api/v1/statistics/options", body, null, true), StatisticsOptionsRef.class);
    }

    List<ModelRef> queryModels(Map<String, Object> payload) {
        return convertList(request("POST", "/api/v1/models/query", payload, null, true), ModelRef.class);
    }

    StatisticsViewRef statistics(Map<String, Object> payload) {
        return convert(request("POST", "/api/v1/models/statistics", payload, null, true), StatisticsViewRef.class);
    }

    ChartViewRef queryChart(Map<String, Object> payload) {
        return convert(request("POST", "/api/v1/statistics/charts/query", payload, null, true), ChartViewRef.class);
    }

    CollectionTaskRef saveCollectionTask(Map<String, Object> payload) {
        return convert(request("POST", "/api/v1/collection-tasks", payload, null, true), CollectionTaskRef.class);
    }

    CollectionTaskRef publishCollectionTask(Long id) {
        return convert(request("POST", "/api/v1/collection-tasks/" + id + "/online", null, null, true), CollectionTaskRef.class);
    }

    void triggerCollectionTask(Long id) {
        request("POST", "/api/v1/collection-tasks/" + id + "/trigger", null, null, true);
    }

    RunListRef listRuns(Long collectionTaskId, Long workflowDefinitionId) {
        Map<String, String> query = new LinkedHashMap<String, String>();
        if (collectionTaskId != null) {
            query.put("collectionTaskId", String.valueOf(collectionTaskId));
        }
        if (workflowDefinitionId != null) {
            query.put("workflowDefinitionId", String.valueOf(workflowDefinitionId));
        }
        return convert(request("GET", "/api/v1/runs", null, query, true), RunListRef.class);
    }

    WorkflowRef saveWorkflow(Map<String, Object> payload) {
        return convert(request("POST", "/api/v1/workflows", payload, null, true), WorkflowRef.class);
    }

    WorkflowRef publishWorkflow(Long id) {
        return convert(request("POST", "/api/v1/workflows/" + id + "/publish", null, null, true), WorkflowRef.class);
    }

    void triggerWorkflow(Long id) {
        request("POST", "/api/v1/workflows/" + id + "/trigger", null, null, true);
    }

    WorkflowRunPageRef listWorkflowRuns(Long workflowDefinitionId, int pageNo, int pageSize) {
        Map<String, String> query = new LinkedHashMap<String, String>();
        if (workflowDefinitionId != null) {
            query.put("workflowDefinitionId", String.valueOf(workflowDefinitionId));
        }
        query.put("pageNo", String.valueOf(pageNo));
        query.put("pageSize", String.valueOf(pageSize));
        return convert(request("GET", "/api/v1/workflow-runs", null, query, true), WorkflowRunPageRef.class);
    }

    private JsonNode request(String method,
                             String path,
                             Object body,
                             Map<String, String> query,
                             boolean includeAuth) {
        HttpURLConnection connection = null;
        try {
            String url = buildUrl(path, query);
            connection = (HttpURLConnection) new URL(url).openConnection();
            connection.setRequestMethod(method);
            connection.setConnectTimeout(timeoutMs);
            connection.setReadTimeout(timeoutMs);
            connection.setUseCaches(false);
            connection.setRequestProperty("Accept", "application/json");
            if (includeAuth) {
                if (token == null || token.trim().isEmpty()) {
                    throw new IllegalStateException("Studio login is required before calling " + path);
                }
                connection.setRequestProperty("Authorization", "Bearer " + token);
                if (tenantId != null && !tenantId.trim().isEmpty()) {
                    connection.setRequestProperty("X-Tenant-Id", tenantId);
                }
                if (projectId != null) {
                    connection.setRequestProperty("X-Project-Id", String.valueOf(projectId));
                }
            }
            if (body != null) {
                connection.setDoOutput(true);
                connection.setRequestProperty("Content-Type", "application/json");
                byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(body);
                OutputStream outputStream = connection.getOutputStream();
                outputStream.write(bytes);
                outputStream.flush();
                outputStream.close();
            }
            int status = connection.getResponseCode();
            InputStream responseStream = status >= 200 && status < 400
                    ? connection.getInputStream()
                    : connection.getErrorStream();
            String responseBody = responseStream == null ? "" : readBody(responseStream);
            if (status < 200 || status >= 300) {
                throw new IllegalStateException("HTTP " + status + " for " + path + ": " + responseBody);
            }
            JsonNode root = responseBody == null || responseBody.trim().isEmpty()
                    ? OBJECT_MAPPER.getNodeFactory().nullNode()
                    : OBJECT_MAPPER.readTree(responseBody);
            if (root.has("success") && !root.path("success").asBoolean(true)) {
                throw new IllegalStateException("Studio request failed for " + path + ": " + root.path("message").asText());
            }
            return root.has("data") ? root.path("data") : root;
        } catch (IOException e) {
            throw new IllegalStateException("Studio request failed for " + path, e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private String buildUrl(String path, Map<String, String> query) throws IOException {
        if (query == null || query.isEmpty()) {
            return baseUrl + path;
        }
        StringBuilder builder = new StringBuilder(baseUrl).append(path).append("?");
        boolean first = true;
        for (Map.Entry<String, String> entry : query.entrySet()) {
            if (!first) {
                builder.append("&");
            }
            builder.append(URLEncoder.encode(entry.getKey(), "UTF-8"))
                    .append("=")
                    .append(URLEncoder.encode(entry.getValue(), "UTF-8"));
            first = false;
        }
        return builder.toString();
    }

    private String readBody(InputStream inputStream) throws IOException {
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while ((read = inputStream.read(buffer)) >= 0) {
                output.write(buffer, 0, read);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        } finally {
            inputStream.close();
        }
    }

    private <T> T convert(JsonNode data, Class<T> type) {
        if (data == null || data.isNull()) {
            return null;
        }
        return OBJECT_MAPPER.convertValue(data, type);
    }

    private <T> List<T> convertList(JsonNode data, Class<T> type) {
        if (data == null || data.isNull() || !data.isArray()) {
            return Collections.emptyList();
        }
        List<T> items = new ArrayList<T>();
        for (JsonNode node : data) {
            items.add(OBJECT_MAPPER.convertValue(node, type));
        }
        return items;
    }

    private String text(JsonNode node, String field) {
        if (node == null || node.isNull()) {
            return null;
        }
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    private Long asLong(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.asLong();
        }
        String value = node.asText();
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return Long.parseLong(value);
    }

    private String firstNonBlank(String first, String second) {
        if (first != null && !first.trim().isEmpty()) {
            return first;
        }
        return second;
    }

    private String trimRightSlash(String value) {
        if (value == null) {
            return null;
        }
        String current = value.trim();
        while (current.endsWith("/")) {
            current = current.substring(0, current.length() - 1);
        }
        return current;
    }

    static final class ProjectRef {
        private Long id;
        private String projectCode;
        private String projectName;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getProjectCode() {
            return projectCode;
        }

        public void setProjectCode(String projectCode) {
            this.projectCode = projectCode;
        }

        public String getProjectName() {
            return projectName;
        }

        public void setProjectName(String projectName) {
            this.projectName = projectName;
        }
    }

    static final class ProjectWorkerRef {
        private Long id;
        private String workerCode;
        private String status;
        private String lastHeartbeatAt;
        private Boolean boundToProject;
        private Boolean enabled;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getWorkerCode() {
            return workerCode;
        }

        public void setWorkerCode(String workerCode) {
            this.workerCode = workerCode;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getLastHeartbeatAt() {
            return lastHeartbeatAt;
        }

        public void setLastHeartbeatAt(String lastHeartbeatAt) {
            this.lastHeartbeatAt = lastHeartbeatAt;
        }

        public Boolean getBoundToProject() {
            return boundToProject;
        }

        public void setBoundToProject(Boolean boundToProject) {
            this.boundToProject = boundToProject;
        }

        public Boolean getEnabled() {
            return enabled;
        }

        public void setEnabled(Boolean enabled) {
            this.enabled = enabled;
        }
    }

    static final class ProjectWorkerBindingRef {
        private Long id;
        private Long projectId;
        private String workerCode;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Long getProjectId() {
            return projectId;
        }

        public void setProjectId(Long projectId) {
            this.projectId = projectId;
        }

        public String getWorkerCode() {
            return workerCode;
        }

        public void setWorkerCode(String workerCode) {
            this.workerCode = workerCode;
        }
    }

    static final class DataSourceRef {
        private Long id;
        private String name;
        private String typeCode;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getTypeCode() {
            return typeCode;
        }

        public void setTypeCode(String typeCode) {
            this.typeCode = typeCode;
        }
    }

    static final class DiscoveryResult {
        private List<ModelRef> models = new ArrayList<ModelRef>();
        private String message;
        private long total;
        private int pageNo;
        private int pageSize;
        private boolean hasMore;

        public List<ModelRef> getModels() {
            return models;
        }

        public void setModels(List<ModelRef> models) {
            this.models = models;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public long getTotal() {
            return total;
        }

        public void setTotal(long total) {
            this.total = total;
        }

        public int getPageNo() {
            return pageNo;
        }

        public void setPageNo(int pageNo) {
            this.pageNo = pageNo;
        }

        public int getPageSize() {
            return pageSize;
        }

        public void setPageSize(int pageSize) {
            this.pageSize = pageSize;
        }

        public boolean isHasMore() {
            return hasMore;
        }

        public void setHasMore(boolean hasMore) {
            this.hasMore = hasMore;
        }
    }

    static final class ModelSyncTaskRef {
        private Long id;
        private String name;
        private String status;
        private Integer totalCount;
        private Integer successCount;
        private Integer failedCount;
        private Integer stoppedCount;
        private Integer progressPercent;
        private String lastError;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public Integer getTotalCount() {
            return totalCount;
        }

        public void setTotalCount(Integer totalCount) {
            this.totalCount = totalCount;
        }

        public Integer getSuccessCount() {
            return successCount;
        }

        public void setSuccessCount(Integer successCount) {
            this.successCount = successCount;
        }

        public Integer getFailedCount() {
            return failedCount;
        }

        public void setFailedCount(Integer failedCount) {
            this.failedCount = failedCount;
        }

        public Integer getStoppedCount() {
            return stoppedCount;
        }

        public void setStoppedCount(Integer stoppedCount) {
            this.stoppedCount = stoppedCount;
        }

        public Integer getProgressPercent() {
            return progressPercent;
        }

        public void setProgressPercent(Integer progressPercent) {
            this.progressPercent = progressPercent;
        }

        public String getLastError() {
            return lastError;
        }

        public void setLastError(String lastError) {
            this.lastError = lastError;
        }
    }

    static final class ModelRef {
        private Long id;
        private Long datasourceId;
        private String name;
        private String modelKind;
        private String physicalLocator;
        private Long schemaVersionId;
        private Map<String, Object> technicalMetadata = new LinkedHashMap<String, Object>();
        private Map<String, Object> businessMetadata = new LinkedHashMap<String, Object>();

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Long getDatasourceId() {
            return datasourceId;
        }

        public void setDatasourceId(Long datasourceId) {
            this.datasourceId = datasourceId;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getModelKind() {
            return modelKind;
        }

        public void setModelKind(String modelKind) {
            this.modelKind = modelKind;
        }

        public String getPhysicalLocator() {
            return physicalLocator;
        }

        public void setPhysicalLocator(String physicalLocator) {
            this.physicalLocator = physicalLocator;
        }

        public Long getSchemaVersionId() {
            return schemaVersionId;
        }

        public void setSchemaVersionId(Long schemaVersionId) {
            this.schemaVersionId = schemaVersionId;
        }

        public Map<String, Object> getTechnicalMetadata() {
            return technicalMetadata;
        }

        public void setTechnicalMetadata(Map<String, Object> technicalMetadata) {
            this.technicalMetadata = technicalMetadata;
        }

        public Map<String, Object> getBusinessMetadata() {
            return businessMetadata;
        }

        public void setBusinessMetadata(Map<String, Object> businessMetadata) {
            this.businessMetadata = businessMetadata;
        }
    }

    static final class MetadataSchemaRef {
        private Long id;
        private Long currentVersionId;
        private String schemaCode;
        private String schemaName;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Long getCurrentVersionId() {
            return currentVersionId;
        }

        public void setCurrentVersionId(Long currentVersionId) {
            this.currentVersionId = currentVersionId;
        }

        public String getSchemaCode() {
            return schemaCode;
        }

        public void setSchemaCode(String schemaCode) {
            this.schemaCode = schemaCode;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }
    }

    static final class IndexQueueStatusRef {
        private Integer queuedRebuildCount;
        private Integer activeRebuildCount;
        private Integer pendingRebuildCount;
        private Integer queuedCommandCount;
        private Integer activeCommandCount;
        private Boolean busy;

        public Integer getQueuedRebuildCount() {
            return queuedRebuildCount;
        }

        public void setQueuedRebuildCount(Integer queuedRebuildCount) {
            this.queuedRebuildCount = queuedRebuildCount;
        }

        public Integer getActiveRebuildCount() {
            return activeRebuildCount;
        }

        public void setActiveRebuildCount(Integer activeRebuildCount) {
            this.activeRebuildCount = activeRebuildCount;
        }

        public Integer getPendingRebuildCount() {
            return pendingRebuildCount;
        }

        public void setPendingRebuildCount(Integer pendingRebuildCount) {
            this.pendingRebuildCount = pendingRebuildCount;
        }

        public Integer getQueuedCommandCount() {
            return queuedCommandCount;
        }

        public void setQueuedCommandCount(Integer queuedCommandCount) {
            this.queuedCommandCount = queuedCommandCount;
        }

        public Integer getActiveCommandCount() {
            return activeCommandCount;
        }

        public void setActiveCommandCount(Integer activeCommandCount) {
            this.activeCommandCount = activeCommandCount;
        }

        public Boolean getBusy() {
            return busy;
        }

        public void setBusy(Boolean busy) {
            this.busy = busy;
        }
    }

    static final class StatisticsOptionsRef {
        private String datasourceType;
        private List<SchemaOptionRef> querySchemas = new ArrayList<SchemaOptionRef>();
        private List<SchemaOptionRef> targetSchemas = new ArrayList<SchemaOptionRef>();

        public String getDatasourceType() {
            return datasourceType;
        }

        public void setDatasourceType(String datasourceType) {
            this.datasourceType = datasourceType;
        }

        public List<SchemaOptionRef> getQuerySchemas() {
            return querySchemas;
        }

        public void setQuerySchemas(List<SchemaOptionRef> querySchemas) {
            this.querySchemas = querySchemas;
        }

        public List<SchemaOptionRef> getTargetSchemas() {
            return targetSchemas;
        }

        public void setTargetSchemas(List<SchemaOptionRef> targetSchemas) {
            this.targetSchemas = targetSchemas;
        }
    }

    static final class SchemaOptionRef {
        private String schemaCode;
        private String schemaName;
        private String scope;
        private String datasourceType;
        private String metaModelCode;
        private String displayMode;
        private List<FieldOptionRef> fields = new ArrayList<FieldOptionRef>();

        public String getSchemaCode() {
            return schemaCode;
        }

        public void setSchemaCode(String schemaCode) {
            this.schemaCode = schemaCode;
        }

        public String getSchemaName() {
            return schemaName;
        }

        public void setSchemaName(String schemaName) {
            this.schemaName = schemaName;
        }

        public String getScope() {
            return scope;
        }

        public void setScope(String scope) {
            this.scope = scope;
        }

        public String getDatasourceType() {
            return datasourceType;
        }

        public void setDatasourceType(String datasourceType) {
            this.datasourceType = datasourceType;
        }

        public String getMetaModelCode() {
            return metaModelCode;
        }

        public void setMetaModelCode(String metaModelCode) {
            this.metaModelCode = metaModelCode;
        }

        public String getDisplayMode() {
            return displayMode;
        }

        public void setDisplayMode(String displayMode) {
            this.displayMode = displayMode;
        }

        public List<FieldOptionRef> getFields() {
            return fields;
        }

        public void setFields(List<FieldOptionRef> fields) {
            this.fields = fields;
        }
    }

    static final class FieldOptionRef {
        private String fieldKey;
        private String fieldName;
        private String valueType;
        private List<String> queryOperators = new ArrayList<String>();
        private String queryDefaultOperator;
        private List<String> supportedChartTypes = new ArrayList<String>();

        public String getFieldKey() {
            return fieldKey;
        }

        public void setFieldKey(String fieldKey) {
            this.fieldKey = fieldKey;
        }

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getValueType() {
            return valueType;
        }

        public void setValueType(String valueType) {
            this.valueType = valueType;
        }

        public List<String> getQueryOperators() {
            return queryOperators;
        }

        public void setQueryOperators(List<String> queryOperators) {
            this.queryOperators = queryOperators;
        }

        public String getQueryDefaultOperator() {
            return queryDefaultOperator;
        }

        public void setQueryDefaultOperator(String queryDefaultOperator) {
            this.queryDefaultOperator = queryDefaultOperator;
        }

        public List<String> getSupportedChartTypes() {
            return supportedChartTypes;
        }

        public void setSupportedChartTypes(List<String> supportedChartTypes) {
            this.supportedChartTypes = supportedChartTypes;
        }
    }

    static final class StatisticsViewRef {
        private Object matchedModelCount;
        private Object matchedItemCount;
        private List<Map<String, Object>> buckets = new ArrayList<Map<String, Object>>();
        private Map<String, Object> summaryMetrics = new LinkedHashMap<String, Object>();

        public Object getMatchedModelCount() {
            return matchedModelCount;
        }

        public void setMatchedModelCount(Object matchedModelCount) {
            this.matchedModelCount = matchedModelCount;
        }

        public Object getMatchedItemCount() {
            return matchedItemCount;
        }

        public void setMatchedItemCount(Object matchedItemCount) {
            this.matchedItemCount = matchedItemCount;
        }

        public List<Map<String, Object>> getBuckets() {
            return buckets;
        }

        public void setBuckets(List<Map<String, Object>> buckets) {
            this.buckets = buckets;
        }

        public Map<String, Object> getSummaryMetrics() {
            return summaryMetrics;
        }

        public void setSummaryMetrics(Map<String, Object> summaryMetrics) {
            this.summaryMetrics = summaryMetrics;
        }
    }

    static final class ChartViewRef {
        private String chartType;
        private Map<String, Object> summaryMetrics = new LinkedHashMap<String, Object>();
        private List<String> xAxis = new ArrayList<String>();
        private List<Map<String, Object>> series = new ArrayList<Map<String, Object>>();
        private List<Map<String, Object>> tableRows = new ArrayList<Map<String, Object>>();
        private String disabledReason;

        public String getChartType() {
            return chartType;
        }

        public void setChartType(String chartType) {
            this.chartType = chartType;
        }

        public Map<String, Object> getSummaryMetrics() {
            return summaryMetrics;
        }

        public void setSummaryMetrics(Map<String, Object> summaryMetrics) {
            this.summaryMetrics = summaryMetrics;
        }

        public List<String> getXAxis() {
            return xAxis;
        }

        public void setXAxis(List<String> xAxis) {
            this.xAxis = xAxis;
        }

        public List<Map<String, Object>> getSeries() {
            return series;
        }

        public void setSeries(List<Map<String, Object>> series) {
            this.series = series;
        }

        public List<Map<String, Object>> getTableRows() {
            return tableRows;
        }

        public void setTableRows(List<Map<String, Object>> tableRows) {
            this.tableRows = tableRows;
        }

        public String getDisabledReason() {
            return disabledReason;
        }

        public void setDisabledReason(String disabledReason) {
            this.disabledReason = disabledReason;
        }
    }

    static final class CollectionTaskRef {
        private Long id;
        private String name;
        private String status;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }
    }

    static final class RunListRef {
        private List<QueuedTaskRef> queuedTasks = new ArrayList<QueuedTaskRef>();
        private List<RunRecordRef> runRecords = new ArrayList<RunRecordRef>();

        public List<QueuedTaskRef> getQueuedTasks() {
            return queuedTasks;
        }

        public void setQueuedTasks(List<QueuedTaskRef> queuedTasks) {
            this.queuedTasks = queuedTasks;
        }

        public List<RunRecordRef> getRunRecords() {
            return runRecords;
        }

        public void setRunRecords(List<RunRecordRef> runRecords) {
            this.runRecords = runRecords;
        }
    }

    static final class QueuedTaskRef {
        private Long collectionTaskId;
        private Long workflowDefinitionId;
        private String status;
        private String nodeCode;

        public Long getCollectionTaskId() {
            return collectionTaskId;
        }

        public void setCollectionTaskId(Long collectionTaskId) {
            this.collectionTaskId = collectionTaskId;
        }

        public Long getWorkflowDefinitionId() {
            return workflowDefinitionId;
        }

        public void setWorkflowDefinitionId(Long workflowDefinitionId) {
            this.workflowDefinitionId = workflowDefinitionId;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getNodeCode() {
            return nodeCode;
        }

        public void setNodeCode(String nodeCode) {
            this.nodeCode = nodeCode;
        }
    }

    static final class RunRecordRef {
        private Long id;
        private Long collectionTaskId;
        private Long workflowDefinitionId;
        private Long workflowRunId;
        private String status;
        private String message;
        private String workerCode;
        private String nodeCode;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public Long getCollectionTaskId() {
            return collectionTaskId;
        }

        public void setCollectionTaskId(Long collectionTaskId) {
            this.collectionTaskId = collectionTaskId;
        }

        public Long getWorkflowDefinitionId() {
            return workflowDefinitionId;
        }

        public void setWorkflowDefinitionId(Long workflowDefinitionId) {
            this.workflowDefinitionId = workflowDefinitionId;
        }

        public Long getWorkflowRunId() {
            return workflowRunId;
        }

        public void setWorkflowRunId(Long workflowRunId) {
            this.workflowRunId = workflowRunId;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }

        public String getWorkerCode() {
            return workerCode;
        }

        public void setWorkerCode(String workerCode) {
            this.workerCode = workerCode;
        }

        public String getNodeCode() {
            return nodeCode;
        }

        public void setNodeCode(String nodeCode) {
            this.nodeCode = nodeCode;
        }
    }

    static final class WorkflowRef {
        private Long id;
        private String code;
        private String name;
        private Boolean published;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Boolean getPublished() {
            return published;
        }

        public void setPublished(Boolean published) {
            this.published = published;
        }
    }

    static final class WorkflowRunPageRef {
        private int pageNo;
        private int pageSize;
        private long total;
        private List<WorkflowRunSummaryRef> items = new ArrayList<WorkflowRunSummaryRef>();

        public int getPageNo() {
            return pageNo;
        }

        public void setPageNo(int pageNo) {
            this.pageNo = pageNo;
        }

        public int getPageSize() {
            return pageSize;
        }

        public void setPageSize(int pageSize) {
            this.pageSize = pageSize;
        }

        public long getTotal() {
            return total;
        }

        public void setTotal(long total) {
            this.total = total;
        }

        public List<WorkflowRunSummaryRef> getItems() {
            return items;
        }

        public void setItems(List<WorkflowRunSummaryRef> items) {
            this.items = items;
        }
    }

    static final class WorkflowRunSummaryRef {
        private Long workflowRunId;
        private Long workflowDefinitionId;
        private String status;
        private Integer successNodes;
        private Integer failedNodes;
        private Integer runningNodes;
        private Integer queuedNodes;

        public Long getWorkflowRunId() {
            return workflowRunId;
        }

        public void setWorkflowRunId(Long workflowRunId) {
            this.workflowRunId = workflowRunId;
        }

        public Long getWorkflowDefinitionId() {
            return workflowDefinitionId;
        }

        public void setWorkflowDefinitionId(Long workflowDefinitionId) {
            this.workflowDefinitionId = workflowDefinitionId;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public Integer getSuccessNodes() {
            return successNodes;
        }

        public void setSuccessNodes(Integer successNodes) {
            this.successNodes = successNodes;
        }

        public Integer getFailedNodes() {
            return failedNodes;
        }

        public void setFailedNodes(Integer failedNodes) {
            this.failedNodes = failedNodes;
        }

        public Integer getRunningNodes() {
            return runningNodes;
        }

        public void setRunningNodes(Integer runningNodes) {
            this.runningNodes = runningNodes;
        }

        public Integer getQueuedNodes() {
            return queuedNodes;
        }

        public void setQueuedNodes(Integer queuedNodes) {
            this.queuedNodes = queuedNodes;
        }
    }
}
