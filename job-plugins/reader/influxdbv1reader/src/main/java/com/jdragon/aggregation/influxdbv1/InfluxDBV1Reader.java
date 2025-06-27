package com.jdragon.aggregation.influxdbv1;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.element.*;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.ColumnInfo;
import com.jdragon.aggregation.datasource.influxdbv1.InfluxDBV1SourcePlugin;
import org.apache.commons.lang3.StringUtils;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class InfluxDBV1Reader extends Reader.Job {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBV1Reader.class);

    private InfluxDB influxDB;

    private BaseDataSourceDTO dataSource;

    private String selectSql;

    private InfluxDBV1SourcePlugin influxDBV1SourcePlugin;

    private List<String> columns;

    @Override
    public void init() {
        this.influxDBV1SourcePlugin = new InfluxDBV1SourcePlugin();
        Configuration pluginJobConf = super.getPluginJobConf();
        Configuration connectConfig = pluginJobConf.getConfiguration("connect");
        dataSource = JSONObject.parseObject(connectConfig.toString(), BaseDataSourceDTO.class);

        influxDB = InfluxDBFactory.connect(dataSource.getHost(), dataSource.getUserName(), dataSource.getPassword());
        influxDB.setDatabase(dataSource.getDatabase());

        selectSql = this.getPluginJobConf().getString("selectSql");
        if (StringUtils.isBlank(selectSql)) {
            String measurement = pluginJobConf.getString("measurement");
            columns = this.getPluginJobConf().getList("columns", String.class);
            if (columns == null || columns.isEmpty()) {
                List<ColumnInfo> columnInfoList = this.influxDBV1SourcePlugin.getColumns(dataSource, measurement);
                columns = columnInfoList.stream().map(ColumnInfo::getColumnName).collect(Collectors.toList());
            }
            selectSql = String.format("SELECT %s FROM \"%s\"", String.join(",", columns), measurement);
        }

        LOG.info("influxdb query: {}", selectSql);
    }

    @Override
    public void prepare() {
        influxDB.ping();  // 简单检测连通性
    }

    @Override
    public void startRead(RecordSender recordSender) {
        Query query = new Query(selectSql, dataSource.getDatabase());
        QueryResult queryResult = influxDB.query(query);

        for (QueryResult.Result result : queryResult.getResults()) {
            if (result.getSeries() != null) {
                for (QueryResult.Series series : result.getSeries()) {
                    List<String> columns = series.getColumns();
                    List<List<Object>> values = series.getValues();

                    for (List<Object> row : values) {
                        Record record = buildRecord(recordSender, columns, row);
                        recordSender.sendToWriter(record);
                    }
                }
            }
        }
    }

    protected Record buildRecord(RecordSender recordSender, List<String> columns, List<Object> row) {
        Record record = recordSender.createRecord();
        for (String column : this.columns) {
            int columnIndex = columns.indexOf(column);
            if (columnIndex != -1) {
                Object value = row.get(columnIndex);
                record.addColumn(convertToColumn(value));
            } else {
                record.addColumn(convertToColumn(null));
            }
        }
        return record;
    }

    private Column convertToColumn(Object value) {
        if (value == null) {
            return new StringColumn();
        }
        if (value instanceof Number) {
            if (value instanceof Double || value instanceof Float) {
                return new DoubleColumn(value.toString());
            } else {
                return new LongColumn(value.toString());
            }
        }
        if (value instanceof Boolean) {
            return new BoolColumn((Boolean) value);
        }
        if (value instanceof String) {
            if (isIso8601Date((String) value)) {
                Instant instant = Instant.parse(value.toString());
                Date date = Date.from(instant);
                return new DateColumn(date);
            }
            return new StringColumn((String) value);
        }
        if (value instanceof Date) {
            return new DateColumn((Date) value);
        }
        // 时间戳在 InfluxDB 通常是 ISO8601 字符串
        return new StringColumn(value.toString());
    }

    private boolean isIso8601Date(String value) {
        // 简单检测
        return value.matches("\\d{4}-\\d{2}-\\d{2}.*");
    }

    @Override
    public void post() {
        // 无需额外操作
    }

    @Override
    public void destroy() {
        if (influxDB != null) {
            influxDB.close();
        }
    }
}
