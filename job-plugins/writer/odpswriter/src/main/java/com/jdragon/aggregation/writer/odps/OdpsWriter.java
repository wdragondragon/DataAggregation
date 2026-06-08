package com.jdragon.aggregation.writer.odps;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.RecordWriter;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.odps.OdpsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class OdpsWriter extends Writer.Job {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsWriter.class);

    static final String WRITE_MODE_APPEND = "append";
    static final String WRITE_MODE_OVERWRITE = "overwrite";
    static final int MAX_BLOCK_COUNT = 20000;

    private BaseDataSourceDTO dataSource;
    private String table;
    private List<String> columns;
    private String partitionSpec;
    private List<String> partitionColumns;
    private String writeMode;
    private int batchSize;
    private boolean emptyAsNull;
    private boolean autoCreatePartition;
    private String preSql;
    private String postSql;

    private Odps odps;
    private TableTunnel tunnel;
    private Table targetTable;
    private TableSchema schema;
    private WritePlan writePlan;
    private final Map<String, UploadContext> uploadContexts = new LinkedHashMap<>();
    private long writtenCount;

    @Override
    public void init() {
        Configuration conf = super.getPluginJobConf();
        Configuration connectConfig = conf.getConfiguration("connect");
        if (connectConfig == null) {
            throw new IllegalArgumentException("Missing odps writer connect config.");
        }
        this.dataSource = parseDataSource(connectConfig);
        if (isBlank(dataSource.getType())) {
            dataSource.setType("odps");
        }
        dataSource.setDatabase(resolveProjectName(dataSource));

        this.table = conf.getString("table");
        this.columns = normalizeColumns(conf.getList("columns", String.class));
        this.partitionSpec = conf.getString("partitionSpec");
        this.partitionColumns = normalizeColumns(conf.getList("partitionColumns", String.class));
        this.writeMode = resolveWriteMode(conf.getString("writeMode", WRITE_MODE_APPEND));
        this.batchSize = conf.getInt("batchSize", 1000);
        this.emptyAsNull = conf.getBool("emptyAsNull", false);
        this.autoCreatePartition = conf.getBool("autoCreatePartition", true);
        this.preSql = conf.getString("preSql");
        this.postSql = conf.getString("postSql");

        validateBasicConfig();
        initOdpsResources();
    }

    @SuppressWarnings("unchecked")
    static BaseDataSourceDTO parseDataSource(Configuration connectConfig) {
        JSONObject connectJson = JSONObject.parseObject(connectConfig.toString());
        Object extraParams = connectJson.get("extraParams");
        if (extraParams instanceof String) {
            String rawExtraParams = (String) extraParams;
            if (isBlank(rawExtraParams)) {
                connectJson.put("extraParams", Collections.emptyMap());
            } else {
                try {
                    extraParams = JSONObject.parseObject(rawExtraParams);
                } catch (Exception e) {
                    throw new IllegalArgumentException("connect.extraParams must be a JSON object string.", e);
                }
            }
        }
        if (extraParams instanceof Map) {
            connectJson.put("extraParams", OdpsUtils.normalizeExtraParams((Map<String, ?>) extraParams));
        }
        return JSONObject.parseObject(connectJson.toJSONString(), BaseDataSourceDTO.class);
    }

    private void validateBasicConfig() {
        validateConnect(dataSource);
        requireNotBlank(table, "table");
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Missing odps writer config: columns");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0");
        }
    }

    static void validateConnect(BaseDataSourceDTO dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("Missing odps connect config.");
        }
        requireNotBlank(dataSource.getHost(), "connect.host");
        requireNotBlank(dataSource.getDatabase(), "connect.database");
        requireNotBlank(dataSource.getUserName(), "connect.userName");
        requireNotBlank(dataSource.getPassword(), "connect.password");
    }

    private void initOdpsResources() {
        try {
            this.odps = OdpsUtils.createOdps(dataSource);
            String project = resolveProjectName(dataSource);
            if (!odps.tables().exists(project, table)) {
                throw new IllegalArgumentException("odps target table not found: " + project + "." + table);
            }
            this.targetTable = odps.tables().get(project, table);
            this.targetTable.reload();
            this.schema = targetTable.getSchema();
            this.writePlan = buildWritePlan(columns, partitionColumns, partitionSpec, schema);

            this.tunnel = new TableTunnel(odps);
            String tunnelEndpoint = getTunnelEndpoint(dataSource);
            if (!isBlank(tunnelEndpoint)) {
                tunnel.setEndpoint(tunnelEndpoint);
            }
            LOG.info("odps writer initialized. project={}, table={}, writeMode={}, batchSize={}, partitioned={}",
                    project, table, writeMode, batchSize, writePlan.partitioned);
        } catch (Exception e) {
            throw new RuntimeException("odps writer init failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void startWrite(RecordReceiver lineReceiver) {
        LOG.info("start to writer odps table={}, writeMode={}", table, writeMode);
        try {
            executeSqlIfPresent(preSql, "preSql");
            com.jdragon.aggregation.commons.element.Record sourceRecord;
            while ((sourceRecord = lineReceiver.getFromReader()) != null) {
                UploadContext context = uploadContext(sourceRecord);
                context.write(sourceRecord);
                writtenCount++;
            }
            commitAll();
            executeSqlIfPresent(postSql, "postSql");
            LOG.info("odps writer finished. table={}, writtenCount={}, sessionCount={}",
                    table, writtenCount, uploadContexts.size());
        } catch (Exception e) {
            closeOpenWritersQuietly();
            throw new RuntimeException("odps write failed: " + e.getMessage(), e);
        }
    }

    private UploadContext uploadContext(com.jdragon.aggregation.commons.element.Record sourceRecord) throws Exception {
        PartitionSpec spec = null;
        String contextKey = "__table__";
        if (writePlan.partitioned) {
            spec = writePlan.staticPartitionSpec != null
                    ? writePlan.staticPartitionSpec
                    : buildDynamicPartitionSpec(writePlan.partitionBindings, sourceRecord);
            contextKey = canonicalPartitionSpec(partitionValues(spec.toString()), writePlan.partitionColumnNames);
        }

        UploadContext context = uploadContexts.get(contextKey);
        if (context == null) {
            context = createUploadContext(spec);
            uploadContexts.put(contextKey, context);
        }
        return context;
    }

    private UploadContext createUploadContext(PartitionSpec spec) throws Exception {
        String project = resolveProjectName(dataSource);
        if (writePlan.partitioned) {
            ensurePartitionExists(spec);
        }

        boolean overwrite = WRITE_MODE_OVERWRITE.equals(writeMode);
        TableTunnel.UploadSession session;
        if (writePlan.partitioned) {
            session = overwrite
                    ? tunnel.createUploadSession(project, table, spec, true)
                    : tunnel.createUploadSession(project, table, spec);
        } else {
            session = overwrite
                    ? tunnel.createUploadSession(project, table, true)
                    : tunnel.createUploadSession(project, table);
        }
        LOG.info("created odps upload session. table={}, partition={}, overwrite={}, sessionId={}",
                table, spec == null ? "" : spec, overwrite, session.getId());
        return new UploadContext(session, writePlan.dataBindings, batchSize, emptyAsNull);
    }

    private void ensurePartitionExists(PartitionSpec spec) throws Exception {
        if (spec == null) {
            return;
        }
        if (targetTable.hasPartition(spec)) {
            return;
        }
        if (!autoCreatePartition) {
            throw new IllegalArgumentException("odps target partition not found: " + spec);
        }
        targetTable.createPartition(spec, true);
        LOG.info("created odps target partition. table={}, partition={}", table, spec);
    }

    private void executeSqlIfPresent(String sql, String configName) throws Exception {
        if (isBlank(sql)) {
            return;
        }
        LOG.info("execute odps writer {}", configName);
        SQLTask.run(odps, normalizeSql(sql)).waitForSuccess();
    }

    private void commitAll() throws Exception {
        if (shouldRejectDynamicPartitionOverwrite(writeMode,
                writePlan != null && !writePlan.partitionBindings.isEmpty(),
                uploadContexts.size())) {
            throw new IllegalArgumentException("odps dynamic partition overwrite touches multiple partitions and is not atomic; "
                    + "split the job by partition, use append, or configure a static partition. partitions="
                    + new ArrayList<>(uploadContexts.keySet()));
        }
        List<String> committedTargets = new ArrayList<>();
        List<String> pendingTargets = new ArrayList<>(uploadContexts.keySet());
        for (Map.Entry<String, UploadContext> entry : uploadContexts.entrySet()) {
            String target = entry.getKey();
            try {
                entry.getValue().commit();
                committedTargets.add(target);
                pendingTargets.remove(target);
            } catch (Exception e) {
                throw new RuntimeException("odps upload session commit failed. committedTargets="
                        + committedTargets + ", pendingTargets=" + pendingTargets
                        + ", reason=" + e.getMessage(), e);
            }
        }
    }

    static boolean shouldRejectDynamicPartitionOverwrite(String writeMode, boolean dynamicPartition, int uploadContextCount) {
        return WRITE_MODE_OVERWRITE.equals(writeMode) && dynamicPartition && uploadContextCount > 1;
    }

    private void closeOpenWritersQuietly() {
        for (UploadContext context : uploadContexts.values()) {
            context.closeOpenWriterQuietly();
        }
    }

    private PartitionSpec buildDynamicPartitionSpec(List<ColumnBinding> partitionBindings,
                                                    com.jdragon.aggregation.commons.element.Record record) {
        Map<String, String> values = new LinkedHashMap<>();
        for (ColumnBinding binding : partitionBindings) {
            com.jdragon.aggregation.commons.element.Column column = columnAt(record, binding);
            String partitionValue = column == null ? null : column.asString();
            if (isBlank(partitionValue)) {
                throw new IllegalArgumentException("Missing odps partition value: " + binding.columnName);
            }
            values.put(binding.columnName, partitionValue);
        }
        return new PartitionSpec(canonicalPartitionSpec(values, writePlan.partitionColumnNames));
    }

    static WritePlan buildWritePlan(List<String> configuredColumns,
                                    List<String> configuredPartitionColumns,
                                    String configuredPartitionSpec,
                                    TableSchema schema) {
        List<String> dataSchemaColumns = schemaDataColumnNames(schema);
        List<String> partitionSchemaColumns = schemaPartitionColumnNames(schema);
        boolean partitioned = !partitionSchemaColumns.isEmpty();
        boolean hasStaticPartition = !isBlank(configuredPartitionSpec);
        boolean hasDynamicPartition = configuredPartitionColumns != null && !configuredPartitionColumns.isEmpty();

        if (!partitioned && (hasStaticPartition || hasDynamicPartition)) {
            throw new IllegalArgumentException("odps target table is not partitioned, partition config is not allowed.");
        }
        if (partitioned && hasStaticPartition && hasDynamicPartition) {
            throw new IllegalArgumentException("partitionSpec and partitionColumns cannot be configured together.");
        }
        if (partitioned && !hasStaticPartition && !hasDynamicPartition) {
            throw new IllegalArgumentException("odps partitioned table requires partitionSpec or partitionColumns.");
        }
        if (hasDynamicPartition && isWildcardColumns(configuredColumns)) {
            throw new IllegalArgumentException("dynamic partition write requires explicit columns, wildcard columns are not allowed.");
        }

        List<String> resolvedColumns = resolveConfiguredColumns(configuredColumns, dataSchemaColumns);
        Map<String, Integer> columnIndex = indexByColumnName(resolvedColumns);

        Set<String> partitionColumnSet = new LinkedHashSet<>(normalizeColumns(configuredPartitionColumns));
        for (String partitionColumn : partitionColumnSet) {
            requireSchemaColumn(partitionSchemaColumns, partitionColumn, "partition column");
            if (!columnIndex.containsKey(partitionColumn)) {
                throw new IllegalArgumentException("partition column is not present in columns: " + partitionColumn);
            }
        }

        PartitionSpec staticPartitionSpec = null;
        if (hasStaticPartition) {
            Map<String, String> staticValues = partitionValues(configuredPartitionSpec);
            requireFullPartitionValues(partitionSchemaColumns, staticValues);
            staticPartitionSpec = new PartitionSpec(canonicalPartitionSpec(staticValues, partitionSchemaColumns));
        }

        if (hasDynamicPartition) {
            requireFullPartitionValues(partitionSchemaColumns, partitionColumnSet);
        }

        List<ColumnBinding> dataBindings = new ArrayList<>();
        List<ColumnBinding> partitionBindings = new ArrayList<>();
        for (String columnName : resolvedColumns) {
            int index = columnIndex.get(columnName);
            if (partitionColumnSet.contains(columnName)) {
                partitionBindings.add(new ColumnBinding(columnName, index, schema.getPartitionColumn(columnName), true));
                continue;
            }
            if (partitionSchemaColumns.contains(columnName)) {
                continue;
            }
            requireSchemaColumn(dataSchemaColumns, columnName, "data column");
            dataBindings.add(new ColumnBinding(columnName, index, schema.getColumn(columnName), false));
        }

        return new WritePlan(partitioned, dataBindings, partitionBindings, partitionSchemaColumns, staticPartitionSpec);
    }

    static List<String> resolveConfiguredColumns(List<String> configuredColumns, List<String> dataSchemaColumns) {
        if (configuredColumns == null || configuredColumns.isEmpty() || isWildcardColumns(configuredColumns)) {
            return dataSchemaColumns;
        }
        return normalizeColumns(configuredColumns);
    }

    static Object toOdpsValue(com.jdragon.aggregation.commons.element.Column source,
                              Column targetColumn,
                              boolean emptyAsNull) {
        Object value = rawValue(source, emptyAsNull);
        if (value == null) {
            return null;
        }
        OdpsType type = targetColumn.getTypeInfo().getOdpsType();
        try {
            switch (type) {
                case BIGINT:
                    return source.asLong();
                case INT:
                    return toInteger(source, targetColumn);
                case SMALLINT:
                    return toShort(source, targetColumn);
                case TINYINT:
                    return toByte(source, targetColumn);
                case DOUBLE:
                    return source.asDouble();
                case FLOAT:
                    Double doubleValue = source.asDouble();
                    return doubleValue == null ? null : doubleValue.floatValue();
                case DECIMAL:
                    return source.asBigDecimal();
                case BOOLEAN:
                    return source.asBoolean();
                case STRING:
                case CHAR:
                case VARCHAR:
                    return source.asString();
                case DATETIME:
                    return source.asDate();
                case DATE:
                    return toSqlDate(source.asDate());
                case TIMESTAMP:
                    return toTimestamp(source.asDate());
                case BINARY:
                    return source.asBytes();
                default:
                    throw unsupportedType(targetColumn, value);
            }
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("convert odps column failed. column="
                    + targetColumn.getName() + ", type=" + type + ", value=" + value, e);
        }
    }

    private static RuntimeException unsupportedType(Column targetColumn, Object value) {
        return new IllegalArgumentException("unsupported odps type. column="
                + targetColumn.getName() + ", type=" + targetColumn.getTypeInfo().getTypeName() + ", value=" + value);
    }

    private static Integer toInteger(com.jdragon.aggregation.commons.element.Column source, Column targetColumn) {
        Long value = source.asLong();
        if (value == null) {
            return null;
        }
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("value overflow for INT column " + targetColumn.getName() + ": " + value);
        }
        return value.intValue();
    }

    private static Short toShort(com.jdragon.aggregation.commons.element.Column source, Column targetColumn) {
        Long value = source.asLong();
        if (value == null) {
            return null;
        }
        if (value < Short.MIN_VALUE || value > Short.MAX_VALUE) {
            throw new IllegalArgumentException("value overflow for SMALLINT column " + targetColumn.getName() + ": " + value);
        }
        return value.shortValue();
    }

    private static Byte toByte(com.jdragon.aggregation.commons.element.Column source, Column targetColumn) {
        Long value = source.asLong();
        if (value == null) {
            return null;
        }
        if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
            throw new IllegalArgumentException("value overflow for TINYINT column " + targetColumn.getName() + ": " + value);
        }
        return value.byteValue();
    }

    private static java.sql.Date toSqlDate(java.util.Date value) {
        return value == null ? null : new java.sql.Date(value.getTime());
    }

    private static Timestamp toTimestamp(java.util.Date value) {
        return value == null ? null : new Timestamp(value.getTime());
    }

    private static Object rawValue(com.jdragon.aggregation.commons.element.Column source, boolean emptyAsNull) {
        if (source == null || source.getRawData() == null) {
            return null;
        }
        if (emptyAsNull && source.asString() != null && source.asString().isEmpty()) {
            return null;
        }
        return source.getRawData();
    }

    private static com.jdragon.aggregation.commons.element.Column columnAt(
            com.jdragon.aggregation.commons.element.Record record,
            ColumnBinding binding) {
        if (binding.index < 0 || binding.index >= record.getColumnNumber()) {
            throw new IllegalArgumentException("record column index out of range. column="
                    + binding.columnName + ", index=" + binding.index + ", recordColumns=" + record.getColumnNumber());
        }
        return record.getColumn(binding.index);
    }

    static String resolveWriteMode(String configuredMode) {
        String mode = isBlank(configuredMode) ? WRITE_MODE_APPEND : configuredMode.trim().toLowerCase(Locale.ENGLISH);
        if (WRITE_MODE_APPEND.equals(mode) || WRITE_MODE_OVERWRITE.equals(mode)) {
            return mode;
        }
        throw new IllegalArgumentException("Unsupported odps writeMode: " + configuredMode);
    }

    static List<String> normalizeColumns(List<String> configuredColumns) {
        List<String> result = new ArrayList<>();
        if (configuredColumns == null) {
            return result;
        }
        Set<String> seen = new LinkedHashSet<>();
        for (String column : configuredColumns) {
            if (!isBlank(column) && seen.add(column.trim())) {
                result.add(column.trim());
            }
        }
        return result;
    }

    static Map<String, String> partitionValues(String spec) {
        Map<String, String> values = new LinkedHashMap<>();
        if (isBlank(spec)) {
            return values;
        }
        String[] parts = spec.split(",");
        for (String part : parts) {
            String[] keyValue = part.split("=", 2);
            if (keyValue.length == 2) {
                values.put(keyValue[0].trim(), stripQuote(keyValue[1].trim()));
            }
        }
        return values;
    }

    static String canonicalPartitionSpec(Map<String, String> values, List<String> partitionColumnNames) {
        StringBuilder builder = new StringBuilder();
        boolean first = true;
        for (String partitionColumn : partitionColumnNames) {
            String value = values.get(partitionColumn);
            if (value == null) {
                throw new IllegalArgumentException("Missing odps partition value: " + partitionColumn);
            }
            if (!first) {
                builder.append(",");
            }
            builder.append(partitionColumn).append("='").append(value.replace("'", "\\'")).append("'");
            first = false;
        }
        return builder.toString();
    }

    static String resolveProjectName(BaseDataSourceDTO dataSource) {
        String projectName = dataSource.getDatabase();
        if (isBlank(projectName)) {
            return projectName;
        }
        String projectEnv = getExtraParam(dataSource, "projectEnv");
        if ("dev".equalsIgnoreCase(projectEnv) && !projectName.endsWith("_dev")) {
            return projectName + "_dev";
        }
        return projectName;
    }

    private static List<String> schemaDataColumnNames(TableSchema schema) {
        List<String> result = new ArrayList<>();
        for (Column column : schema.getColumns()) {
            result.add(column.getName());
        }
        return result;
    }

    private static List<String> schemaPartitionColumnNames(TableSchema schema) {
        List<String> result = new ArrayList<>();
        for (Column column : schema.getPartitionColumns()) {
            result.add(column.getName());
        }
        return result;
    }

    private static Map<String, Integer> indexByColumnName(List<String> columns) {
        Map<String, Integer> result = new LinkedHashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            result.put(columns.get(i), i);
        }
        return result;
    }

    private static void requireFullPartitionValues(List<String> partitionSchemaColumns, Map<String, String> values) {
        for (String partitionColumn : partitionSchemaColumns) {
            if (!values.containsKey(partitionColumn) || isBlank(values.get(partitionColumn))) {
                throw new IllegalArgumentException("Missing odps partition value: " + partitionColumn);
            }
        }
    }

    private static void requireFullPartitionValues(List<String> partitionSchemaColumns, Set<String> values) {
        for (String partitionColumn : partitionSchemaColumns) {
            if (!values.contains(partitionColumn)) {
                throw new IllegalArgumentException("Missing odps partition column: " + partitionColumn);
            }
        }
    }

    private static void requireSchemaColumn(List<String> schemaColumns, String columnName, String type) {
        if (!schemaColumns.contains(columnName)) {
            throw new IllegalArgumentException("Unknown odps " + type + ": " + columnName);
        }
    }

    private static boolean isWildcardColumns(List<String> columns) {
        return columns != null && columns.size() == 1 && "*".equals(columns.get(0));
    }

    private static String getExtraParam(BaseDataSourceDTO dataSource, String key) {
        if (dataSource.getExtraParams() == null) {
            return null;
        }
        return dataSource.getExtraParams().get(key);
    }

    private static String getTunnelEndpoint(BaseDataSourceDTO dataSource) {
        String tunnelEndpoint = getExtraParam(dataSource, "tunnelEndpoint");
        if (isBlank(tunnelEndpoint)) {
            tunnelEndpoint = getExtraParam(dataSource, "TunnelEndpoint");
        }
        return tunnelEndpoint;
    }

    private static String stripQuote(String value) {
        if (value == null || value.length() < 2) {
            return value;
        }
        if ((value.startsWith("'") && value.endsWith("'"))
                || (value.startsWith("\"") && value.endsWith("\""))) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }

    private static String normalizeSql(String value) {
        String result = value.trim();
        while (result.endsWith(";")) {
            result = result.substring(0, result.length() - 1).trim();
        }
        return result + ";";
    }

    private static void requireNotBlank(String value, String name) {
        if (isBlank(value)) {
            throw new IllegalArgumentException("Missing odps writer config: " + name);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    @Override
    public void post() {
        // do nothing
    }

    @Override
    public void destroy() {
        closeOpenWritersQuietly();
    }

    static final class WritePlan {
        final boolean partitioned;
        final List<ColumnBinding> dataBindings;
        final List<ColumnBinding> partitionBindings;
        final List<String> partitionColumnNames;
        final PartitionSpec staticPartitionSpec;

        WritePlan(boolean partitioned,
                  List<ColumnBinding> dataBindings,
                  List<ColumnBinding> partitionBindings,
                  List<String> partitionColumnNames,
                  PartitionSpec staticPartitionSpec) {
            this.partitioned = partitioned;
            this.dataBindings = Collections.unmodifiableList(new ArrayList<>(dataBindings));
            this.partitionBindings = Collections.unmodifiableList(new ArrayList<>(partitionBindings));
            this.partitionColumnNames = Collections.unmodifiableList(new ArrayList<>(partitionColumnNames));
            this.staticPartitionSpec = staticPartitionSpec;
        }
    }

    static final class ColumnBinding {
        final String columnName;
        final int index;
        final Column targetColumn;
        final boolean partitionColumn;

        ColumnBinding(String columnName, int index, Column targetColumn, boolean partitionColumn) {
            this.columnName = columnName;
            this.index = index;
            this.targetColumn = targetColumn;
            this.partitionColumn = partitionColumn;
        }
    }

    static final class UploadContext {
        private final TableTunnel.UploadSession session;
        private final List<ColumnBinding> dataBindings;
        private final int batchSize;
        private final boolean emptyAsNull;
        private final List<Long> blockIds = new ArrayList<>();
        private RecordWriter currentWriter;
        private long currentBlockId;
        private int currentBlockRows;

        UploadContext(TableTunnel.UploadSession session,
                      List<ColumnBinding> dataBindings,
                      int batchSize,
                      boolean emptyAsNull) {
            this.session = session;
            this.dataBindings = dataBindings;
            this.batchSize = batchSize;
            this.emptyAsNull = emptyAsNull;
        }

        void write(com.jdragon.aggregation.commons.element.Record sourceRecord) throws Exception {
            ensureWriter();
            com.aliyun.odps.data.Record targetRecord = session.newRecord();
            for (ColumnBinding binding : dataBindings) {
                com.jdragon.aggregation.commons.element.Column sourceColumn = columnAt(sourceRecord, binding);
                targetRecord.set(binding.columnName, toOdpsValue(sourceColumn, binding.targetColumn, emptyAsNull));
            }
            currentWriter.write(targetRecord);
            currentBlockRows++;
            if (currentBlockRows >= batchSize) {
                flushBlock();
            }
        }

        void commit() throws Exception {
            flushBlock();
            if (!blockIds.isEmpty()) {
                session.commit(blockIds.toArray(new Long[0]));
            }
        }

        void closeOpenWriterQuietly() {
            if (currentWriter == null) {
                return;
            }
            try {
                currentWriter.close();
            } catch (Exception ignored) {
                // ignore close errors during cleanup
            } finally {
                currentWriter = null;
            }
        }

        private void ensureWriter() throws Exception {
            if (currentWriter != null) {
                return;
            }
            if (currentBlockId >= MAX_BLOCK_COUNT) {
                throw new IllegalArgumentException("odps upload block count reaches " + MAX_BLOCK_COUNT
                        + ", increase batchSize to reduce block count.");
            }
            currentWriter = session.openRecordWriter(currentBlockId);
        }

        private void flushBlock() throws IOException {
            if (currentWriter == null) {
                return;
            }
            currentWriter.close();
            currentWriter = null;
            if (currentBlockRows > 0) {
                blockIds.add(currentBlockId);
                currentBlockId++;
                currentBlockRows = 0;
            }
        }
    }
}
