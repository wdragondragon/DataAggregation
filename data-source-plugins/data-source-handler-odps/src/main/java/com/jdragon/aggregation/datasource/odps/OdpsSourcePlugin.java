package com.jdragon.aggregation.datasource.odps;

import com.aliyun.odps.*;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import com.jdragon.aggregation.datasource.*;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.*;

@Slf4j
public class OdpsSourcePlugin extends AbstractDataSourcePlugin {

    @Override
    public List<String> getTableNames(BaseDataSourceDTO dataSource, String keyword) {
        Odps odps = OdpsUtils.createOdps(dataSource);
        List<String> tableNames = new ArrayList<>();
        TableFilter filter = new TableFilter();
        filter.setName(keyword);
        for (Iterator<Table> it = odps.tables().iterator(filter); it.hasNext(); ) {
            Table t = it.next();
            tableNames.add(t.getName());
        }
        return tableNames;
    }

    @Override
    public List<TableInfo> getTableInfos(BaseDataSourceDTO dataSource, String table) {
        Odps odps = OdpsUtils.createOdps(dataSource);
        List<TableInfo> list = new ArrayList<>();
        try {
            for (Table tb : odps.tables()) {
                if (tb.getName().contains(table)) {
                    TableInfo info = new TableInfo();
                    info.setTableName(tb.getName());
                    info.setExternalTable(tb.isExternalTable());
                    info.setPartitioned(tb.isPartitioned());
                    info.setRemarks(tb.getComment());
                    list.add(info);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("获取表信息失败: " + e.getMessage(), e);
        }
        return list;
    }

    @Override
    public List<ColumnInfo> getColumns(BaseDataSourceDTO dataSource, String table) {
        Odps odps = OdpsUtils.createOdps(dataSource);
        TableSchema schema = odps.tables().get(table).getSchema();
        List<ColumnInfo> columnInfos = new ArrayList<>();
        int ordinalPosition = 1;
        for (Column col : schema.getColumns()) {
            ColumnInfo info = new ColumnInfo();
            info.setColumnName(col.getName());
            info.setNullable(col.isNullable() ? 1 : 0);
            info.setIsNullable(col.isNullable() ? "YES" : "NO");
            info.setColumnDef(col.getDefaultValue());
            info.setTableName(table);
            info.setTypeName(col.getTypeInfo().getTypeName());
            info.setRemarks(col.getComment());
            info.setOrdinalPosition(ordinalPosition++);
            columnInfos.add(info);
        }
        List<Column> partitionColumns = schema.getPartitionColumns();
        if (partitionColumns != null) {
            for (Column col : partitionColumns) {
                ColumnInfo info = new ColumnInfo();
                info.setColumnName(col.getName());
                info.setNullable(0);
                info.setIsNullable("NO");
                info.setTableName(table);
                info.setTypeName(col.getTypeInfo().getTypeName());
                info.setRemarks(col.getComment());
                info.setOrdinalPosition(ordinalPosition++);
                info.setPartitionColumn(true);
                columnInfos.add(info);
            }
        }
        return columnInfos;
    }

    @Override
    public com.jdragon.aggregation.commons.pagination.Table<Map<String, Object>> dataModelPreview(BaseDataSourceDTO dataSource, String tableName, String limitSize) {
        int limit = parsePreviewLimit(limitSize);
        Odps odps = OdpsUtils.createOdps(dataSource);
        String project = resolveProjectName(dataSource);
        com.jdragon.aggregation.commons.pagination.Table<Map<String, Object>> previewTable =
                new com.jdragon.aggregation.commons.pagination.Table<>();
        TunnelRecordReader reader = null;
        try {
            Table odpsTable = odps.tables().get(project, tableName);
            odpsTable.reload();
            TableSchema schema = odpsTable.getSchema();
            addPreviewHeaders(previewTable, schema);
            if (limit <= 0) {
                return previewTable;
            }

            PartitionSpec previewPartition = null;
            if (odpsTable.isPartitioned()) {
                Partition partition = selectPreviewPartition(odpsTable);
                if (partition == null) {
                    return previewTable;
                }
                previewPartition = partition.getPartitionSpec();
            }

            TableTunnel tunnel = new TableTunnel(odps);
            String tunnelEndpoint = getTunnelEndpoint(dataSource);
            if (!isBlank(tunnelEndpoint)) {
                tunnel.setEndpoint(tunnelEndpoint);
            }
            TableTunnel.DownloadSession session = previewPartition == null
                    ? tunnel.createDownloadSession(project, tableName)
                    : tunnel.createDownloadSession(project, tableName, previewPartition);
            long count = Math.min(session.getRecordCount(), limit);
            if (count <= 0) {
                return previewTable;
            }
            Map<String, String> partitionValues = previewPartition == null
                    ? Collections.<String, String>emptyMap()
                    : partitionValues(previewPartition);
            List<Map<String, Object>> rows = new ArrayList<>();
            reader = session.openRecordReader(0, count);
            Record record;
            while ((record = reader.read()) != null) {
                rows.add(buildPreviewRow(record, schema, partitionValues));
            }
            previewTable.setBodies(rows);
            return previewTable;
        } catch (Exception e) {
            throw new RuntimeException("ODPS样例数据预览失败: " + e.getMessage(), e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception e) {
                    log.warn("关闭ODPS样例预览reader失败", e);
                }
            }
        }
    }

    private static void addPreviewHeaders(com.jdragon.aggregation.commons.pagination.Table<Map<String, Object>> table,
                                          TableSchema schema) {
        for (Column column : schema.getColumns()) {
            table.addHeader(column.getName(), column.getName());
        }
        for (Column column : schema.getPartitionColumns()) {
            table.addHeader(column.getName(), column.getName());
        }
    }

    private static Map<String, Object> buildPreviewRow(Record record,
                                                       TableSchema schema,
                                                       Map<String, String> partitionValues) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (Column column : schema.getColumns()) {
            row.put(column.getName(), normalizeOdpsValue(record.get(column.getName())));
        }
        for (Column column : schema.getPartitionColumns()) {
            row.put(column.getName(), partitionValues.get(column.getName()));
        }
        return row;
    }

    private static Object normalizeOdpsValue(Object value) {
        if ("\\N".equalsIgnoreCase(String.valueOf(value))) {
            return "";
        }
        return value;
    }

    private static Partition selectPreviewPartition(Table table) throws OdpsException {
        Partition selected = null;
        for (Partition partition : table.getPartitions()) {
            if (selected == null) {
                selected = partition;
                continue;
            }
            Date selectedTime = selected.getLastDataModifiedTime();
            Date candidateTime = partition.getLastDataModifiedTime();
            if (candidateTime != null && (selectedTime == null || candidateTime.after(selectedTime))) {
                selected = partition;
            }
        }
        return selected;
    }

    private static Map<String, String> partitionValues(PartitionSpec partitionSpec) {
        Map<String, String> values = new LinkedHashMap<>();
        if (partitionSpec == null) {
            return values;
        }
        for (String key : partitionSpec.keys()) {
            values.put(key, partitionSpec.get(key));
        }
        return values;
    }

    private static int parsePreviewLimit(String limitSize) {
        if (limitSize == null || limitSize.trim().isEmpty()) {
            return 10;
        }
        try {
            return Math.max(0, Integer.parseInt(limitSize.trim()));
        } catch (NumberFormatException e) {
            return 10;
        }
    }

    private static String resolveProjectName(BaseDataSourceDTO dataSource) {
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

    private static String getTunnelEndpoint(BaseDataSourceDTO dataSource) {
        String tunnelEndpoint = getExtraParam(dataSource, "tunnelEndpoint");
        if (isBlank(tunnelEndpoint)) {
            tunnelEndpoint = getExtraParam(dataSource, "TunnelEndpoint");
        }
        return tunnelEndpoint;
    }

    private static String getExtraParam(BaseDataSourceDTO dataSource, String key) {
        if (dataSource.getExtraParams() == null) {
            return null;
        }
        return dataSource.getExtraParams().get(key);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static com.jdragon.aggregation.commons.pagination.Table<Map<String, Object>> buildMapByRecords(List<Record> records) {
        com.jdragon.aggregation.commons.pagination.Table<Map<String, Object>> table = new com.jdragon.aggregation.commons.pagination.Table<>();
        List<Map<String, Object>> listMap = new ArrayList<>();
        List<String> columnsList = new ArrayList<>();
        for (Record record : records) {
            Column[] columns = record.getColumns();
            Map<String, Object> map = new LinkedHashMap<>();
            for (Column column : columns) {
                String name = column.getName();
                if (!columnsList.contains(name)) {
                    columnsList.add(name);
                }
                Object value = record.get(name);
                // maxCompute里面的空返回的是使用\n
                if ("\\N".equalsIgnoreCase(String.valueOf(value))) {
                    map.put(name, "");
                } else {
                    map.put(name, value);
                }
            }
            listMap.add(map);
        }
        for (String columnName : columnsList) {
            table.addHeader(columnName, columnName);
        }
        table.setBodies(listMap);
        return table;
    }

    private String generateInsertSql(InsertDataDTO insertDataDTO) {
        // 根据插入数据构造SQL
        StringBuilder sql = new StringBuilder("INSERT INTO " + insertDataDTO.getTableName() + " (" +
                String.join(", ", insertDataDTO.getField()) + ") VALUES ");
        for (List<String> data : insertDataDTO.getData()) {
            sql.append("(")
                    .append(String.join(", ", data))
                    .append("),");
        }
        sql.deleteCharAt(sql.length() - 1);  // 移除最后一个逗号
        return sql.toString();
    }

    @Override
    public String getTableSize(BaseDataSourceDTO dataSource, String table) {
        // ODPS 没有传统意义上的“表大小”，可以返回 null 或者实现估算逻辑
        Odps odps = OdpsUtils.createOdps(dataSource);
        try {
            if (odps.tables().exists(table)) {
                Table odpsTable = odps.tables().get(table);
                odpsTable.reload();
                return String.valueOf(odpsTable.getSize());
            } else {
                return "0";
            }
        } catch (OdpsException e) {
            log.error("获取odps表信息异常", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public Long getTableCount(BaseDataSourceDTO dataSource, String table) {
        String sql = "SELECT COUNT(*) AS cnt FROM " + table;
        com.jdragon.aggregation.commons.pagination.Table<Map<String, Object>> executed = executeQuerySql(dataSource, sql, true);
        List<Map<String, Object>> bodies = executed.getBodies();
        return Long.valueOf(bodies.get(0).get("cnt").toString());
    }

    @Override
    public void insertData(InsertDataDTO insertDataDTO) {
        // 插入数据的具体实现
        try {
            String sql = generateInsertSql(insertDataDTO);
            executeUpdate(insertDataDTO.getBaseDataSourceDTO(), sql);
        } catch (Exception e) {
            log.error("数据插入失败: {}", e.getMessage(), e);
            throw new RuntimeException("数据插入失败: " + e.getMessage(), e);
        }
    }

    // 以下方法因 ODPS 特性不支持
    @Override
    public Connection getConnection(BaseDataSourceDTO dataSource) {
        return null;
    }

    @Override
    public com.jdragon.aggregation.commons.pagination.Table<Map<String, Object>> executeQuerySql(BaseDataSourceDTO dataSource, String sql, boolean columnLabel) {
        Odps odps = OdpsUtils.createOdps(dataSource);
        Instance instance;
        try {
            if (!sql.endsWith(";")) {
                sql += ";";
            }
            instance = SQLTask.run(odps, sql);
            instance.waitForSuccess();
            // 封装返回结果
            List<Record> records = SQLTask.getResult(instance);
            return buildMapByRecords(records);
        } catch (OdpsException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void executeUpdate(BaseDataSourceDTO dataSource, String sql) {
        Odps odps = OdpsUtils.createOdps(dataSource);
        try {
            Instance instance = SQLTask.run(odps, sql);
            instance.waitForSuccess();
        } catch (Exception e) {
            log.error("执行更新失败: {}", e.getMessage(), e);
            throw new RuntimeException("执行更新失败: " + e.getMessage(), e);
        }
    }

    @Override
    public void executeBatch(BaseDataSourceDTO dataSource, List<String> sqlList) {
        Odps odps = OdpsUtils.createOdps(dataSource);
        try {
            for (String sql : sqlList) {
                Instance instance = SQLTask.run(odps, sql);
                instance.waitForSuccess();
            }
        } catch (Exception e) {
            log.error("批量执行失败: {}", e.getMessage(), e);
            throw new RuntimeException("批量执行失败: " + e.getMessage(), e);
        }
    }

    @Override
    public List<PartitionInfo> getPartitionInfo(BaseDataSourceDTO dataSource, String table) {
        List<PartitionInfo> partitionInfos = new ArrayList<>();
        Odps odps = OdpsUtils.createOdps(dataSource);
        Table odpsTable = odps.tables().get(table);
        try {
            if (odpsTable.isPartitioned()) {
                for (Partition partition : odpsTable.getPartitions()) {
                    PartitionSpec partitionSpec = partition.getPartitionSpec();
                    Map<String, String> partitionKv = new HashMap<>();
                    for (String key : partitionSpec.keys()) {
                        partitionKv.put(key, partitionSpec.get(key));
                    }
                    PartitionInfo partitionInfo = new PartitionInfo(partitionKv);
                    partitionInfo.setSize(partition.getSize());
                    partitionInfo.setLastMetaModifiedTime(partition.getLastMetaModifiedTime());
                    partitionInfo.setLastDataModifiedTime(partition.getLastDataModifiedTime());
                    partitionInfos.add(partitionInfo);
                }
            }
        } catch (OdpsException e) {
            throw new RuntimeException(e);
        }
        return partitionInfos;
    }
}
