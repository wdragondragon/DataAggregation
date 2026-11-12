package com.jdragon.aggregation.odps;

import com.alibaba.fastjson.JSONObject;
import com.aliyun.odps.Column;
import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.Partition;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.InstanceTunnel;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.jdragon.aggregation.commons.element.DoubleColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.util.DynamicFunctionResolver;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.utils.ColumnUtils;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.odps.OdpsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class OdpsReader extends Reader.Job {

    private static final Logger LOG = LoggerFactory.getLogger(OdpsReader.class);

    static final String READ_MODE_AUTO = "auto";
    static final String READ_MODE_SQL = "sql";
    static final String READ_MODE_TUNNEL = "tunnel";
    private static final String NULL_VALUE = "\\N";

    private BaseDataSourceDTO dataSource;
    private String configuredReadMode;
    private String readMode;
    private String selectSql;
    private String table;
    private String partitionSpec;
    private List<String> columns;
    private boolean includePartitionColumns;
    private boolean autoPartitionScan;
    private volatile boolean cancelled;
    private long offset;
    private long maxRows;

    @Override
    public void init() {
        cancelled = false;
        Configuration connectConfig = this.getPluginJobConf().getConfiguration("connect");
        if (connectConfig == null) {
            throw new IllegalArgumentException("Missing odps reader connect config.");
        }
        dataSource = parseDataSource(connectConfig);
        if (isBlank(dataSource.getType())) {
            dataSource.setType("odps");
        }
        dataSource.setDatabase(resolveProjectName(dataSource));

        selectSql = this.getPluginJobConf().getString("selectSql");
        configuredReadMode = this.getPluginJobConf().getString("readMode", READ_MODE_AUTO);
        readMode = resolveReadMode(configuredReadMode, selectSql);
        table = this.getPluginJobConf().getString("table");
        partitionSpec = this.getPluginJobConf().getString("partitionSpec");
        columns = normalizeColumns(this.getPluginJobConf().getList("columns", String.class));
        includePartitionColumns = this.getPluginJobConf().getBool("includePartitionColumns", false);
        autoPartitionScan = this.getPluginJobConf().getBool("autoPartitionScan", false);
        offset = this.getPluginJobConf().getLong("offset", 0L);
        maxRows = this.getPluginJobConf().getLong("maxRows", 0L);
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

    @Override
    public void prepare() {
        prepare(LocalDateTime.now());
    }

    void prepare(LocalDateTime baseTime) {
        validateConnect(dataSource);
        validateReadModeSqlCompatibility(configuredReadMode, selectSql);
        if (READ_MODE_SQL.equals(readMode)) {
            if (isBlank(selectSql)) {
                partitionSpec = resolveDynamicFunctions(partitionSpec, "partitionSpec", baseTime);
            } else {
                selectSql = resolveDynamicFunctions(selectSql, "selectSql", baseTime);
            }
            selectSql = resolveSelectSql(selectSql, table, columns, partitionSpec);
        } else {
            partitionSpec = resolveDynamicFunctions(partitionSpec, "partitionSpec", baseTime);
            requireNotBlank(table, "table");
        }
        validateReadWindow(offset, maxRows);
    }

    private static String resolveDynamicFunctions(String value, String fieldName, LocalDateTime baseTime) {
        try {
            return DynamicFunctionResolver.replaceAll(value, baseTime, DynamicFunctionResolver.Mode.STRICT);
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException("Invalid dynamic function in odps " + fieldName + ": "
                    + exception.getMessage(), exception);
        }
    }

    @Override
    public void startRead(RecordSender recordSender) {
        if (READ_MODE_SQL.equals(readMode)) {
            startSqlRead(recordSender);
            return;
        }
        startTunnelRead(recordSender);
    }

    private void startSqlRead(RecordSender recordSender) {
        TunnelRecordReader reader = null;
        try {
            Odps odps = OdpsUtils.createOdps(dataSource);
            Instance instance = SQLTask.run(odps, selectSql);
            instance.waitForSuccess();

            InstanceTunnel.DownloadSession downloadSession = createSqlDownloadSession(odps, instance);
            long total = downloadSession.getRecordCount();
            long readCount = readCount(total, offset, maxRows);
            if (readCount <= 0) {
                LOG.info("odps sql reader has no rows to read. instance={}, total={}, offset={}",
                        instance.getId(), total, offset);
                return;
            }

            TableSchema schema = downloadSession.getSchema();
            List<String> outputColumns = resolveTunnelColumns(columns, schema, false);

            reader = downloadSession.openRecordReader(offset, readCount);
            Record odpsRecord;
            while ((odpsRecord = reader.read()) != null) {
                com.jdragon.aggregation.commons.element.Record record = recordSender.createRecord();
                addColumns(record, outputColumns, odpsRecord, Collections.<String, String>emptyMap());
                recordSender.sendToWriter(record);
            }
        } catch (Exception e) {
            throw new RuntimeException("odps sql read failed: " + e.getMessage(), e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception e) {
                    LOG.warn("close odps sql reader failed", e);
                }
            }
        }
    }

    private void startTunnelRead(RecordSender recordSender) {
        PartitionSpec currentPartition = null;
        long started = System.currentTimeMillis();
        ReadWindow window = new ReadWindow(offset, maxRows);
        try {
            Odps odps = OdpsUtils.createOdps(dataSource);
            List<PartitionSpec> partitions = resolveTunnelPartitions(odps);
            checkCancelled();
            LOG.info("odps tunnel read started. project={}, table={}, partitions={}, offset={}, maxRows={}",
                    resolveProjectName(dataSource), table, partitions.size(), offset, maxRows);
            for (int i = 0; i < partitions.size() && !window.isComplete(); i++) {
                checkCancelled();
                currentPartition = partitions.get(i);
                long partitionStarted = System.currentTimeMillis();
                long emittedBefore = window.emitted;
                readTunnelPartition(odps, currentPartition, recordSender, window);
                checkCancelled();
                LOG.info("odps tunnel partition completed. table={}, partition={}, position={}/{}, rows={}, emitted={}, elapsedMs={}",
                        table, currentPartition, i + 1, partitions.size(), window.emitted - emittedBefore,
                        window.emitted, System.currentTimeMillis() - partitionStarted);
            }
            checkCancelled();
            LOG.info("odps tunnel read completed. project={}, table={}, emitted={}, elapsedMs={}",
                    resolveProjectName(dataSource), table, window.emitted, System.currentTimeMillis() - started);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException("odps tunnel read failed. table=" + table + ", partition="
                    + currentPartition + ", emitted=" + window.emitted + ": " + e.getMessage(), e);
        }
    }

    List<PartitionSpec> resolveTunnelPartitions(Odps odps) throws Exception {
        checkCancelled();
        if (!isBlank(partitionSpec)) {
            return Collections.singletonList(new PartitionSpec(partitionSpec));
        }
        Table odpsTable = odps.tables().get(resolveProjectName(dataSource), table);
        odpsTable.reload();
        checkCancelled();
        if (!odpsTable.isPartitioned()) {
            return Collections.singletonList(null);
        }
        if (!autoPartitionScan) {
            throw new IllegalArgumentException("ODPS table " + table
                    + " is partitioned. Configure partitionSpec or enable autoPartitionScan.");
        }
        // The SDK iterator uses marker pagination with 1000 partitions per page and no total cap.
        return collectPartitions(odpsTable.getPartitionIterator(), odpsTable.getSchema().getPartitionColumns());
    }

    List<PartitionSpec> collectPartitions(Iterator<Partition> iterator, List<Column> partitionColumns)
            throws InterruptedException {
        List<PartitionSpec> partitions = new ArrayList<>();
        while (true) {
            checkCancelled();
            if (!iterator.hasNext()) {
                break;
            }
            checkCancelled();
            PartitionSpec source = iterator.next().getPartitionSpec();
            PartitionSpec normalized = new PartitionSpec();
            for (Column column : partitionColumns) {
                normalized.set(column.getName(), source.get(column.getName()));
            }
            partitions.add(normalized);
        }
        checkCancelled();
        partitions.sort(Comparator.comparing(PartitionSpec::toString));
        checkCancelled();
        return partitions;
    }

    void readTunnelPartition(Odps odps, PartitionSpec partition, RecordSender recordSender, ReadWindow window)
            throws Exception {
        checkCancelled();
        TableTunnel.DownloadSession downloadSession = createDownloadSession(odps, partition);
        checkCancelled();
        long total = downloadSession.getRecordCount();
        long start = Math.min(window.remainingOffset, total);
        long count = window.readCount(total);
        if (count <= 0) {
            return;
        }
        List<String> outputColumns = resolveTunnelColumns(columns, downloadSession.getSchema(), includePartitionColumns);
        Map<String, String> partitionValues = partitionValues(partition);
        checkCancelled();
        RecordReader reader = null;
        try {
            reader = openTunnelRecordReader(downloadSession, start, count);
            Record odpsRecord;
            long partitionEmitted = 0;
            while (true) {
                checkCancelled();
                odpsRecord = reader.read();
                checkCancelled();
                if (odpsRecord == null) {
                    break;
                }
                // Read through EOF so the SDK validates its stream count and checksum.
                if (partitionEmitted >= count || window.isComplete()) {
                    throw new IllegalStateException("ODPS download returned more records than requested");
                }
                com.jdragon.aggregation.commons.element.Record record = recordSender.createRecord();
                addColumns(record, outputColumns, odpsRecord, partitionValues);
                recordSender.sendToWriter(record);
                window.emitted++;
                partitionEmitted++;
            }
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception e) {
                    LOG.warn("close odps tunnel reader failed", e);
                }
            }
        }
    }

    RecordReader openTunnelRecordReader(TableTunnel.DownloadSession session, long start, long count) throws Exception {
        return session.openRecordReader(start, count);
    }

    private TableTunnel.DownloadSession createDownloadSession(Odps odps, PartitionSpec partition) throws Exception {
        String project = resolveProjectName(dataSource);
        TableTunnel tunnel = new TableTunnel(odps);
        String tunnelEndpoint = getTunnelEndpoint(dataSource);
        if (!isBlank(tunnelEndpoint)) {
            tunnel.setEndpoint(tunnelEndpoint);
        }

        if (partition == null) {
            return tunnel.createDownloadSession(project, table);
        }
        return tunnel.createDownloadSession(project, table, partition);
    }

    private void checkCancelled() throws InterruptedException {
        if (cancelled || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("ODPS partition read interrupted");
        }
    }

    static final class ReadWindow {
        private long remainingOffset;
        private final long maxRows;
        private long emitted;

        ReadWindow(long offset, long maxRows) {
            validateReadWindow(offset, maxRows);
            this.remainingOffset = offset;
            this.maxRows = maxRows;
        }

        long readCount(long total) {
            if (isComplete()) {
                return 0;
            }
            long count = OdpsReader.readCount(total, remainingOffset, maxRows > 0 ? maxRows - emitted : 0);
            remainingOffset -= Math.min(remainingOffset, total);
            return count;
        }

        boolean isComplete() {
            return maxRows > 0 && emitted >= maxRows;
        }
    }

    private InstanceTunnel.DownloadSession createSqlDownloadSession(Odps odps, Instance instance) throws Exception {
        InstanceTunnel tunnel = new InstanceTunnel(odps);
        String tunnelEndpoint = getTunnelEndpoint(dataSource);
        if (!isBlank(tunnelEndpoint)) {
            tunnel.setEndpoint(tunnelEndpoint);
        }
        // false disables the SDK's READ_TABLE_MAX_ROW-style 10k result limit for SQL result downloads.
        return tunnel.createDownloadSession(instance.getProject(), instance.getId(), false);
    }

    static String resolveReadMode(String configuredMode, String selectSql) {
        String mode = isBlank(configuredMode) ? READ_MODE_AUTO : configuredMode.trim().toLowerCase();
        if (READ_MODE_AUTO.equals(mode)) {
            return isBlank(selectSql) ? READ_MODE_TUNNEL : READ_MODE_SQL;
        }
        if (READ_MODE_SQL.equals(mode) || READ_MODE_TUNNEL.equals(mode)) {
            return mode;
        }
        throw new IllegalArgumentException("Unsupported odps readMode: " + configuredMode);
    }

    static void validateReadModeSqlCompatibility(String configuredMode, String selectSql) {
        String mode = isBlank(configuredMode) ? READ_MODE_AUTO : configuredMode.trim().toLowerCase();
        if (READ_MODE_TUNNEL.equals(mode) && !isBlank(selectSql)) {
            throw new IllegalArgumentException("odps selectSql requires readMode=sql or readMode=auto.");
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

    static void validateReadWindow(long offset, long maxRows) {
        if (offset < 0) {
            throw new IllegalArgumentException("offset must be >= 0");
        }
        if (maxRows < 0) {
            throw new IllegalArgumentException("maxRows must be >= 0");
        }
    }

    static List<String> normalizeColumns(List<String> configuredColumns) {
        List<String> result = new ArrayList<>();
        if (configuredColumns == null) {
            return result;
        }
        for (String column : configuredColumns) {
            if (!isBlank(column)) {
                result.add(column.trim());
            }
        }
        return result;
    }

    static List<String> resolveTunnelColumns(List<String> configuredColumns, TableSchema schema, boolean includePartitionColumns) {
        if (configuredColumns != null && !configuredColumns.isEmpty() && !isWildcardColumns(configuredColumns)) {
            return unique(configuredColumns);
        }

        Set<String> outputColumns = new LinkedHashSet<>();
        for (Column column : schema.getColumns()) {
            outputColumns.add(column.getName());
        }
        if (includePartitionColumns) {
            for (Column column : schema.getPartitionColumns()) {
                outputColumns.add(column.getName());
            }
        }
        return new ArrayList<>(outputColumns);
    }

    static List<String> resolveSqlColumns(Map<String, Object> row, List<String> configuredColumns) {
        if (configuredColumns != null && !configuredColumns.isEmpty() && !isWildcardColumns(configuredColumns)) {
            return unique(configuredColumns);
        }
        return new ArrayList<>(row.keySet());
    }

    static void addColumns(com.jdragon.aggregation.commons.element.Record target, List<String> outputColumns, Map<String, ?> row) {
        for (String column : outputColumns) {
            target.addColumn(toColumn(row.get(column)));
        }
    }

    static void addColumns(com.jdragon.aggregation.commons.element.Record target,
                           List<String> outputColumns,
                           Record row,
                           Map<String, String> partitionValues) {
        for (String column : outputColumns) {
            Object value;
            if (partitionValues.containsKey(column)) {
                value = partitionValues.get(column);
            } else {
                value = row.get(column);
            }
            target.addColumn(toColumn(value));
        }
    }

    static com.jdragon.aggregation.commons.element.Column toColumn(Object value) {
        Object normalized = normalizeOdpsValue(value);
        if (normalized instanceof BigDecimal || normalized instanceof BigInteger) {
            return new DoubleColumn(normalized.toString());
        }
        return ColumnUtils.object2Column(normalized);
    }

    static Object normalizeOdpsValue(Object value) {
        if (value == null) {
            return null;
        }
        if (NULL_VALUE.equalsIgnoreCase(String.valueOf(value))) {
            return "";
        }
        return value;
    }

    static Map<String, String> partitionValues(String partitionSpec) {
        Map<String, String> values = new LinkedHashMap<>();
        if (isBlank(partitionSpec)) {
            return values;
        }
        String[] parts = partitionSpec.split(",");
        for (String part : parts) {
            String[] keyValue = part.split("=", 2);
            if (keyValue.length == 2) {
                values.put(keyValue[0].trim(), stripQuote(keyValue[1].trim()));
            }
        }
        return values;
    }

    static Map<String, String> partitionValues(PartitionSpec partitionSpec) {
        Map<String, String> values = new LinkedHashMap<>();
        if (partitionSpec != null) {
            for (String key : partitionSpec.keys()) {
                values.put(key, partitionSpec.get(key));
            }
        }
        return values;
    }

    static String resolveSelectSql(String configuredSql, String table, List<String> columns, String partitionSpec) {
        if (!isBlank(configuredSql)) {
            return normalizeSelectSql(configuredSql);
        }
        requireNotBlank(table, "selectSql or table");
        return buildSelectSql(table, columns, partitionSpec);
    }

    static String buildSelectSql(String table, List<String> columns, String partitionSpec) {
        requireNotBlank(table, "table");
        StringBuilder sql = new StringBuilder();
        sql.append("select ");
        if (columns == null || columns.isEmpty() || isWildcardColumns(columns)) {
            sql.append("*");
        } else {
            sql.append(String.join(", ", unique(columns)));
        }
        sql.append(" from ").append(table.trim());

        Map<String, String> partitions = partitionValues(partitionSpec);
        if (!partitions.isEmpty()) {
            sql.append(" where ");
            boolean first = true;
            for (Map.Entry<String, String> entry : partitions.entrySet()) {
                if (!first) {
                    sql.append(" and ");
                }
                sql.append(entry.getKey()).append(" = ").append(sqlStringLiteral(entry.getValue()));
                first = false;
            }
        }
        sql.append(";");
        return sql.toString();
    }

    static long readCount(long total, long offset, long maxRows) {
        validateReadWindow(offset, maxRows);
        if (total <= 0 || offset >= total) {
            return 0L;
        }
        long available = total - offset;
        return maxRows > 0 ? Math.min(available, maxRows) : available;
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

    private static List<String> unique(List<String> columns) {
        return new ArrayList<>(new LinkedHashSet<>(columns));
    }

    private static boolean isWildcardColumns(List<String> columns) {
        return columns.size() == 1 && "*".equals(columns.get(0));
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

    private static String sqlStringLiteral(String value) {
        if (value == null) {
            return "null";
        }
        return "'" + value.replace("'", "''") + "'";
    }

    static String normalizeSelectSql(String value) {
        if (value == null) {
            return null;
        }
        String result = value.trim();
        while (result.endsWith(";")) {
            result = result.substring(0, result.length() - 1).trim();
        }
        if (isBlank(result)) {
            return result;
        }
        result = result + ";";
        return result;
    }

    private static void requireNotBlank(String value, String name) {
        if (isBlank(value)) {
            throw new IllegalArgumentException("Missing odps reader config: " + name);
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
        // The SDK may clear a thread interrupt while retrying a network request.
        cancelled = true;
    }
}
