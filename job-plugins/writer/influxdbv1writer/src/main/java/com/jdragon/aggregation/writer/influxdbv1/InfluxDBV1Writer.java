package com.jdragon.aggregation.writer.influxdbv1;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.influxdbv1.InfluxDBV1SourcePlugin;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class InfluxDBV1Writer extends Writer.Job {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBV1Writer.class);

    private InfluxDB influxDB;

    private BaseDataSourceDTO dataSource;

    private InfluxDBV1SourcePlugin influxDBV1SourcePlugin;

    private List<ColumnEntry> columns;

    private String measurement;

    private int batchSize;

    @Override
    public void init() {
        this.influxDBV1SourcePlugin = new InfluxDBV1SourcePlugin();
        Configuration pluginJobConf = super.getPluginJobConf();
        Configuration connectConfig = pluginJobConf.getConfiguration("connect");
        dataSource = JSONObject.parseObject(connectConfig.toString(), BaseDataSourceDTO.class);

        batchSize = this.getPluginJobConf().getInt("batchSize", 500);
        influxDB = InfluxDBFactory.connect(dataSource.getHost(), dataSource.getUserName(), dataSource.getPassword());
        influxDB.setDatabase(dataSource.getDatabase());
        influxDB.enableBatch(batchSize, 1000, TimeUnit.MILLISECONDS);

        measurement = pluginJobConf.getString("measurement");
        columns = ColumnEntry.getListColumnEntry(pluginJobConf, "columns");
        LOG.info("InfluxDB v1 writer initialized: db={}, measurement={}, batchSize={}", dataSource.getDatabase(), measurement, batchSize);

    }

    @Override
    public void prepare() {
        influxDB.ping();
        createDatabaseIfNotExists(influxDB, dataSource.getDatabase());
    }

    private void createDatabaseIfNotExists(InfluxDB influxDB, String databaseName) {
        // 查询数据库列表
        QueryResult result = influxDB.query(new Query("SHOW DATABASES"));
        boolean exists = result.getResults()
                .stream()
                .flatMap(r -> r.getSeries() != null ? r.getSeries().stream() : java.util.stream.Stream.empty())
                .flatMap(s -> s.getValues().stream())
                .anyMatch(v -> v.contains(databaseName));

        if (!exists) {
            LOG.info("Create database {}", databaseName);
            String createDatabaseQuery = String.format("CREATE DATABASE \"%s\"", databaseName);
            influxDB.query(new Query(createDatabaseQuery));
        }
    }


    @Override
    public void startWrite(RecordReceiver recordReceiver) {
        try {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                Point.Builder pointBuilder = Point.measurement(measurement);
                Long pointTime = null;
                for (int i = 0; i < columns.size(); i++) {
                    Column column = record.getColumn(i);
                    ColumnEntry columnEntry = columns.get(i);
                    String columnName = columnEntry.getName();
                    String type = columnEntry.getType();
                    Object value = column.getRawData();

                    if (value != null) {
                        switch (type) {
                            case "field":
                                if (value instanceof Number) {
                                    pointBuilder.addField(columnName, ((Number) value).doubleValue());
                                } else if (value instanceof Boolean) {
                                    pointBuilder.addField(columnName, (Boolean) value);
                                } else {
                                    pointBuilder.addField(columnName, value.toString());
                                }
                                break;
                            case "tag":
                                pointBuilder.tag(columnName, value.toString());
                                break;
                            case "time":
                                if (value instanceof Number) {
                                    pointTime = ((Number) value).longValue();
                                } else {
                                    // 尝试 parse 日期字符串，或抛异常
                                    try {
                                        pointTime = javax.xml.bind.DatatypeConverter.parseDateTime(value.toString()).getTimeInMillis();
                                    } catch (Exception e) {
                                        throw new IllegalArgumentException("Invalid time format: " + value);
                                    }
                                }
                                break;
                            default:
                                throw new IllegalArgumentException("Unsupported column type: " + type);
                        }
                    }
                }

                if (pointTime == null) {
                    pointTime = System.currentTimeMillis();
                }

                pointBuilder.time(pointTime, TimeUnit.MILLISECONDS);

                influxDB.write(pointBuilder.build());
            }
            influxDB.flush();
        } catch (Exception e) {
            throw AggregationException.asException(InfluxDBV1ErrorCode.INFLUX_WRITE_ERROR, "INFLUX_WRITE_ERROR", e);
        }
    }

    @Override
    public void post() {
        // 可扩展清理逻辑
    }

    @Override
    public void destroy() {
        if (influxDB != null) {
            influxDB.close();
        }
    }
}
