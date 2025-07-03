package com.jdragon.aggregation.datasource.influxdbv1;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class Test {
    static String URL = "http://192.168.43.161:8087";

    static String USERNAME = "admin";

    static String PASSWORD = "zhjl951753";

    static String DATABASE = "mydb";

    public static void main(String[] args) {
        try (InfluxDB influxDB = InfluxDBFactory.connect(URL, USERNAME, PASSWORD);) {
            influxDB.setDatabase(DATABASE);
            Point point = Point.measurement("cpu_load_v2")
                    .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                    .addField("value", 0.68)
                    .tag("host", "server03")
                    .tag("address", "cn-north")
                    .build();

            influxDB.write(point);

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
}
