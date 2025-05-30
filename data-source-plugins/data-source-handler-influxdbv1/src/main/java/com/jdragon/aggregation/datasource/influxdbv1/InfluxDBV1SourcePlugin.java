package com.jdragon.aggregation.datasource.influxdbv1;

import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.datasource.*;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;

public class InfluxDBV1SourcePlugin extends AbstractDataSourcePlugin {

    private static final String UNSUPPORTED = "该操作不支持 InfluxDB 数据源";

    static String URL = "http://172.20.10.2:8087";

    static String USERNAME = "admin";

    static String PASSWORD = "zhjl951753";

    static String DATABASE = "mydb";

    public static void main(String[] args) {
        try (InfluxDB influxDB = connect(URL, USERNAME, PASSWORD)) {
            influxDB.setDatabase(DATABASE);
//            Point point = Point.measurement("cpu_load_v2")
//                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
//                    .addField("value", 0.64)
//                    .tag("host", "server02")
//                    .tag("address", "cn-north")
//                    .build();
//
//            influxDB.write(point);

//            Query query = new Query("SHOW TAG KEYS FROM cpu_load_v2");
            Query query = new Query("SHOW FIELD KEYS FROM cpu_load_v2");
            QueryResult results = influxDB.query(query);

            for (QueryResult.Result result : results.getResults()) {
                for (QueryResult.Series series : result.getSeries()) {
                    List<List<Object>> values = series.getValues();
                    List<String> columns = series.getColumns();
                    for (List<Object> row : values) {
                        for (int i = 0; i < columns.size(); i++) {
                            System.out.println(columns.get(i) + ": " + row.get(i));
                        }
                    }
                }
            }
        }
    }


    public static InfluxDB connect(String url, String username, String password) {
        InfluxDB influxDB = InfluxDBFactory.connect(url, username, password);
        Pong pong = influxDB.ping();
        if (pong.getVersion().equalsIgnoreCase("unknown")) {
            throw new RuntimeException("Failed to connect to InfluxDB");
        }
        return influxDB;
    }

    private InfluxDB connect(BaseDataSourceDTO dataSource) {
        // 这里 dataSource.getHost() = URL，getUsername(), getPassword()
        return InfluxDBFactory.connect(dataSource.getHost(), dataSource.getUserName(), dataSource.getPassword());
    }

    @Override
    public Table<Map<String, Object>> executeQuerySql(BaseDataSourceDTO dataSource, String sql, boolean columnLabel) {
        try (InfluxDB influxDB = connect(dataSource)) {
            influxDB.setDatabase(dataSource.getDatabase());
            Query query = new Query(sql, dataSource.getDatabase());
            QueryResult queryResult = influxDB.query(query);

            return convertToTable(queryResult);
        }
    }

    private Table<Map<String, Object>> convertToTable(QueryResult queryResult) {
        Table<Map<String, Object>> table = new Table<>();
        List<String> headers = new ArrayList<>();
        List<Map<String, Object>> bodies = new ArrayList<>();

        for (QueryResult.Result result : queryResult.getResults()) {
            if (result.getSeries() == null) continue;
            for (QueryResult.Series series : result.getSeries()) {
                List<String> columns = series.getColumns();
                for (List<Object> values : series.getValues()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 0; i < columns.size(); i++) {
                        row.put(columns.get(i), values.get(i));
                        if (!headers.contains(columns.get(i))) {
                            headers.add(columns.get(i));
                        }
                    }
                    // 添加tags
                    if (series.getTags() != null) {
                        row.putAll(series.getTags());
                        for (String tagKey : series.getTags().keySet()) {
                            if (!headers.contains(tagKey)) {
                                headers.add(tagKey);
                            }
                        }
                    }
                    bodies.add(row);
                }
            }
        }
        headers.forEach(h -> table.addHeader(h, h));
        table.setBodies(bodies);
        return table;
    }

    @Override
    public List<String> getTableNames(BaseDataSourceDTO dataSource, String table) {
        // InfluxQL: SHOW MEASUREMENTS
        String sql = "SHOW MEASUREMENTS";
        Table<Map<String, Object>> result = executeQuerySql(dataSource, sql, true);
        return result.getBodies().stream()
                .map(row -> row.get("name").toString())
                .collect(Collectors.toList());
    }

    @Override
    public List<ColumnInfo> getColumns(BaseDataSourceDTO dataSource, String table) {
        List<ColumnInfo> columns = new ArrayList<>();

        try (InfluxDB influxDB = connect(dataSource)) {
            influxDB.setDatabase(dataSource.getDatabase());

            // 获取 tag keys
            Query tagQuery = new Query("SHOW TAG KEYS FROM \"" + table + "\"", dataSource.getDatabase());
            QueryResult tagResult = influxDB.query(tagQuery);
            List<String> tagKeys = new ArrayList<>();
            for (QueryResult.Result result : tagResult.getResults()) {
                if (result.getSeries() == null) continue;
                for (QueryResult.Series series : result.getSeries()) {
                    tagKeys.addAll(series.getValues().stream()
                            .map(row -> row.get(0).toString())
                            .collect(Collectors.toList()));
                }
            }
            for (String tag : tagKeys) {
                ColumnInfo columnInfo = new ColumnInfo();
                columnInfo.setColumnName(tag);
                columnInfo.setIndexType("tag");
                columnInfo.setTypeName("string");
                columns.add(columnInfo);
            }

            // 获取 field keys
            Query fieldQuery = new Query("SHOW FIELD KEYS FROM \"" + table + "\"", dataSource.getDatabase());
            QueryResult fieldResult = influxDB.query(fieldQuery);
            for (QueryResult.Result result : fieldResult.getResults()) {
                if (result.getSeries() == null) {
                    continue;
                }
                for (QueryResult.Series series : result.getSeries()) {
                    series.getValues().forEach(row -> {
                        ColumnInfo columnInfo = new ColumnInfo();
                        columnInfo.setColumnName(row.get(0).toString());
                        columnInfo.setTypeName(row.get(1).toString());
                        columnInfo.setIndexType("field");
                    });
                }
            }
        }
        return columns;
    }


    @Override
    public void executeUpdate(BaseDataSourceDTO dataSource, String sql) {
        // InfluxDB v1 也不支持标准更新
        throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public void executeBatch(BaseDataSourceDTO dataSource, List<String> sqlList) {
        throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public Table<Map<String, Object>> dataModelPreview(BaseDataSourceDTO dataSource, String tableName, String limitSize) {
        // SELECT * FROM "measurement" LIMIT n
        String sql = String.format("SELECT * FROM \"%s\" LIMIT %s", tableName, limitSize);
        return executeQuerySql(dataSource, sql, true);
    }

    @Override
    public void insertData(InsertDataDTO insertDataDTO) {
//        try (InfluxDB influxDB = connect(insertDataDTO.getBaseDataSourceDTO())) {
//            influxDB.setDatabase(insertDataDTO.getBaseDataSourceDTO().getDatabase());
//
//            Point point = Point.measurement(insertDataDTO.getTableName())
//                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
//                    .fields(insertDataDTO.getFields())
//                    .tag(insertDataDTO.getTags())
//                    .build();
//
//            influxDB.write(point);
//        }
    }

    @Override
    public List<TableInfo> getTableInfos(BaseDataSourceDTO dataSource, String table) {
        List<String> measurements = getTableNames(dataSource, table);
        List<TableInfo> tableInfos = new ArrayList<>();
        for (String measurement : measurements) {
            TableInfo info = new TableInfo();
            info.setTableName(measurement);
            tableInfos.add(info);
        }
        return tableInfos;
    }

    @Override
    public String getTableSize(BaseDataSourceDTO dataSource, String table) {
        throw new UnsupportedOperationException(UNSUPPORTED);
    }

    @Override
    public Long getTableCount(BaseDataSourceDTO dataSource, String table) {
        String sql = String.format("SELECT COUNT(*) FROM \"%s\"", table);
        Table<Map<String, Object>> result = executeQuerySql(dataSource, sql, true);
        Optional<Object> countVal = result.getBodies().stream()
                .map(row -> row.get("count_value")) // 不同measurement具体字段名不一，可能需要调整
                .filter(Objects::nonNull)
                .findFirst();
        Double v = countVal.map(o -> Double.parseDouble(o.toString())).orElse(0.0);
        return v.longValue();
    }

    @Override
    public Connection getConnection(BaseDataSourceDTO dataSource) {
        throw new UnsupportedOperationException(UNSUPPORTED);
    }
}
