package com.jdragon.aggregation.datasource.influxdb;

import com.influxdb.client.*;
import com.influxdb.client.domain.Bucket;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.datasource.*;
import com.jdragon.aggregation.datasource.scan.TableScanReadable;

import java.sql.Connection;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class InfluxDBSourcePlugin extends AbstractDataSourcePlugin implements TableScanReadable {

    static String URL = "http://172.20.10.2:8086";

    static String TOKEN = "8FtG2nRdBqxxOtQaQfIHFCmNpsxClb9yvaqs3XbHmMlEbeSZdNlz69MSK7rORbKYEguGE82pBgHJlkQeRi4CJw==";

    static String ORG = "jdragon_org";

    static String BUCKET = "test";

    public static void main(String[] args) {
        try (InfluxDBClient client = InfluxDBClientFactory.create(URL, TOKEN.toCharArray(), ORG, BUCKET)) {
            QueryApi queryApi = client.getQueryApi();
            BucketsApi bucketsApi = client.getBucketsApi();
            List<Bucket> buckets = bucketsApi.findBuckets();
            for (Bucket bucket : buckets) {
                System.out.println("当前 bucket: " + bucket.getName());
                String flux = "import \"influxdata/influxdb/schema\"\n"
                        + "schema.measurements(bucket: \"" + bucket.getName() + "\")";

                List<FluxTable> query = queryApi.query(flux);
                for (FluxTable table : query) {
                    List<String> columnsName;
                    List<String> tagsName;
                    List<String> measurementsName = table.getRecords().stream()
                            .map(fluxRecord -> fluxRecord.getValueByKey("_value").toString()).collect(Collectors.toList());
                    for (String measurementName : measurementsName) {
                        System.out.println("当前 measurement name: " + measurementName);
                        String tagQuery = "import \"influxdata/influxdb/schema\"\n"
                                + "schema.tagKeys(bucket: \"" + bucket.getName() + "\", predicate: (r) => r._measurement == \"" + measurementName + "\", start: 0)";
                        String fieldQuery = "import \"influxdata/influxdb/schema\"\n"
                                + "schema.fieldKeys(bucket: \"" + bucket.getName() + "\", predicate: (r) => r._measurement == \"" + measurementName + "\", start: 0)";
                        System.out.println("🔖 Tags:");
                        for (FluxTable tag : queryApi.query(tagQuery)) {
                            tagsName = tag.getRecords().stream()
                                    .map(fluxRecord -> Objects.requireNonNull(fluxRecord.getValueByKey("_value")).toString())
                                    .collect(Collectors.toList());
                            tagsName.forEach(System.out::println);
                        }

                        System.out.println("📦 Fields:");
                        for (FluxTable fields : queryApi.query(fieldQuery)) {
                            columnsName = fields.getRecords().stream()
                                    .map(fluxRecord -> Objects.requireNonNull(fluxRecord.getValueByKey("_value")).toString())
                                    .collect(Collectors.toList());
                            columnsName.forEach(System.out::println);
                        }
                        String queryTableData = "from(bucket: \"" + bucket.getName() + "\")\n" +
                                "    |> range(start: 0)\n" +
                                "    |> filter(fn: (r) => r._measurement == \"" + measurementName + "\")\n" +
                                "    |> pivot(rowKey: [\"_time\"], columnKey: [\"_field\"], valueColumn: \"_value\")";
                        for (FluxTable records : queryApi.query(queryTableData)) {
                            for (FluxRecord record : records.getRecords()) {
//                            Map<String, Object> values = new HashMap<>();
//                            for (String columnName : columnsName) {
//                                values.put(columnName, record.getValueByKey(columnName));
//                            }
//                            values.put("_time", record.getValueByKey("_time"));
                                Map<String, Object> values = record.getValues();
                                System.out.println(values);
                            }
                        }
                    }
                }
            }
        }
    }

    private static final String UNSUPPORTED = "该操作不支持 InfluxDB 数据源";

    @Override
    public Connection getConnection(BaseDataSourceDTO dataSource) {
        throw new UnsupportedOperationException(UNSUPPORTED); // InfluxDB 不提供 JDBC 连接
    }

    @Override
    public Table<Map<String, Object>> executeQuerySql(BaseDataSourceDTO dataSource, String sql, boolean columnLabel) {
        // 执行 Flux 查询
        InfluxDBClient client = createClient(dataSource);
        QueryApi queryApi = client.getQueryApi();

        List<FluxTable> tables = queryApi.query(sql);
        Table<Map<String, Object>> result = convertToTable(tables);
        client.close();
        return result;
    }

    @Override
    public void scanRows(BaseDataSourceDTO dataSource, String querySql, boolean columnLabel, Consumer<Map<String, Object>> rowConsumer) {
        scanQuery(dataSource, querySql, columnLabel, rowConsumer);
    }

    @Override
    public void scanQuery(BaseDataSourceDTO dataSource, String sql, boolean columnLabel, Consumer<Map<String, Object>> rowConsumer) {
        if (rowConsumer == null) {
            return;
        }
        try (InfluxDBClient client = createClient(dataSource)) {
            QueryApi queryApi = client.getQueryApi();
            for (FluxTable fluxTable : queryApi.query(sql)) {
                for (FluxRecord record : fluxTable.getRecords()) {
                    rowConsumer.accept(new LinkedHashMap<>(record.getValues()));
                }
            }
        }
    }

    private Table<Map<String, Object>> convertToTable(List<FluxTable> tables) {
        Table<Map<String, Object>> table = new Table<>();
        List<String> headerNameList = new ArrayList<>();
        List<Map<String, Object>> body = new ArrayList<>();
        for (FluxTable fluxTable : tables) {
            List<FluxRecord> records = fluxTable.getRecords();
            for (FluxRecord record : records) {
                Map<String, Object> values = record.getValues();
                body.add(values);
                for (String field : values.keySet()) {
                    if (!headerNameList.contains(field)) {
                        headerNameList.add(field);
                    }
                }
            }
        }
        headerNameList.forEach(headerName -> table.addHeader(headerName, headerName));
        table.setBodies(body);
        return table;
    }

    @Override
    public void executeUpdate(BaseDataSourceDTO dataSource, String sql) {
        throw new UnsupportedOperationException(UNSUPPORTED); // 没有更新语句
    }

    @Override
    public void executeBatch(BaseDataSourceDTO dataSource, List<String> sqlList) {
        throw new UnsupportedOperationException(UNSUPPORTED); // 不支持 SQL 批处理
    }

    @Override
    public Table<Map<String, Object>> dataModelPreview(BaseDataSourceDTO dataSource, String tableName, String limitSize) {
        // 类似 SELECT * FROM measurement LIMIT X
        String flux = String.format("from(bucket:\"%s\") |> range(start: 0) |> filter(fn: (r) => r._measurement == \"%s\") |> limit(n:%s)", dataSource.getBucket(), tableName, limitSize);
        return executeQuerySql(dataSource, flux, true);
    }

    @Override
    public void insertData(InsertDataDTO insertDataDTO) {
        // 插入点数据
//        InfluxDBClient client = createClient(insertDataDTO.getBaseDataSourceDTO());
//        WriteApiBlocking writeApi = client.getWriteApiBlocking();
//
//        // 转换数据格式后写入（取决于你 insertDataDTO 的结构）
//        Point point = Point.measurement(insertDataDTO.getTableName())
//                .addTags(insertDataDTO.getTags())
//                .addFields(insertDataDTO.getFields())
//                .time(Instant.now(), WritePrecision.NS);
//
//        writeApi.writePoint(point);
//        client.close();
    }

    @Override
    public List<TableInfo> getTableInfos(BaseDataSourceDTO dataSource, String table) {
        // 可视为 measurement 信息
        List<String> measurements = getTableNames(dataSource, table);
        List<TableInfo> tableInfos = new ArrayList<>();
        for (String measurement : measurements) {
            TableInfo tableInfo = new TableInfo();
            tableInfo.setTableName(measurement);
            tableInfos.add(tableInfo);
        }
        return tableInfos;
    }

    @Override
    public List<String> getTableNames(BaseDataSourceDTO dataSource, String table) {
        // Flux 查询 schema.measurements
        String flux = String.format("import \"influxdata/influxdb/schema\"\nschema.measurements(bucket: \"%s\")", dataSource.getBucket());
        Table<Map<String, Object>> result = executeQuerySql(dataSource, flux, true);
        return result.getBodies().stream().map(row -> row.get("_value").toString()).collect(Collectors.toList());
    }

    @Override
    public List<ColumnInfo> getColumns(BaseDataSourceDTO dataSource, String table) {
        // 获取 tags 和 fields 合并返回
        List<ColumnInfo> columns = new ArrayList<>();

        // Tags

        String tagFlux = String.format("import \"influxdata/influxdb/schema\"\nschema.tagKeys(bucket: \"%s\", predicate: (r) => r._measurement == \"%s\", start: 0)", dataSource.getBucket(), table);
        List<String> tagKeys = executeQuerySql(dataSource, tagFlux, true)
                .getBodies().stream().map(r -> r.get("_value").toString()).collect(Collectors.toList());

        for (String tag : tagKeys) {
            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setColumnName(tag);
            columnInfo.setIndexType("tag");
            columns.add(columnInfo);
        }

        // Fields
        String fieldFlux = String.format("import \"influxdata/influxdb/schema\"\nschema.fieldKeys(bucket: \"%s\", predicate: (r) => r._measurement == \"%s\", start: 0)", dataSource.getBucket(), table);
        List<String> fieldKeys = executeQuerySql(dataSource, fieldFlux, true)
                .getBodies().stream().map(r -> r.get("_value").toString()).collect(Collectors.toList());

        for (String field : fieldKeys) {
            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setColumnName(field);
            columnInfo.setIndexType("field");
            columns.add(columnInfo);
        }

        return columns;
    }

    @Override
    public String getTableSize(BaseDataSourceDTO dataSource, String table) {
        throw new UnsupportedOperationException(UNSUPPORTED); // 不支持统计表空间大小
    }

    @Override
    public Long getTableCount(BaseDataSourceDTO dataSource, String table) {
        // 执行 count 查询
        String flux = String.format("from(bucket: \"%s\") |> range(start: 0) |> filter(fn: (r) => r._measurement == \"%s\") |> count()", dataSource.getBucket(), table);
        Table<Map<String, Object>> result = executeQuerySql(dataSource, flux, true);
        Object value = result.getBodies().stream().findFirst().map(r -> r.get("_value")).orElse(0);
        return Long.parseLong(value.toString());
    }

    // 工具方法：构建客户端
    private InfluxDBClient createClient(BaseDataSourceDTO dataSource) {
        return InfluxDBClientFactory.create(
                dataSource.getHost(),
                dataSource.getPassword().toCharArray(),
                dataSource.getDatabase(),
                dataSource.getBucket()
        );
    }
}
