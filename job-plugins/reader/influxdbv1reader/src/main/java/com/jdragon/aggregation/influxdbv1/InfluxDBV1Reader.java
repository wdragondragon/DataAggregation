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
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InfluxDBV1Reader extends Reader.Job {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDBV1Reader.class);

    private InfluxDB influxDB;

    private BaseDataSourceDTO dataSource;

    private InfluxDBV1SourcePlugin influxDBV1SourcePlugin;

    private List<String> columns;

    private String measurement;

    private Instant startTime;

    private Instant endTime;

    private long windowSizeMs;

    private int pageLimit;

    private String customSql;

    @Override
    public void init() {
        this.influxDBV1SourcePlugin = new InfluxDBV1SourcePlugin();
        Configuration pluginJobConf = super.getPluginJobConf();
        Configuration connectConfig = pluginJobConf.getConfiguration("connect");
        dataSource = JSONObject.parseObject(connectConfig.toString(), BaseDataSourceDTO.class);

        influxDB = InfluxDBFactory.connect(dataSource.getHost(), dataSource.getUserName(), dataSource.getPassword());
        influxDB.setDatabase(dataSource.getDatabase());
        columns = this.getPluginJobConf().getList("columns", String.class);
        customSql = this.getPluginJobConf().getString("selectSql");
        if (StringUtils.isBlank(customSql)) {
            measurement = pluginJobConf.getString("measurement");
            if (columns == null || columns.isEmpty()) {
                List<ColumnInfo> columnInfoList = this.influxDBV1SourcePlugin.getColumns(dataSource, measurement);
                columns = columnInfoList.stream().map(ColumnInfo::getColumnName).collect(Collectors.toList());
            }
            String startTimeStr = pluginJobConf.getString("startTime");
            if (StringUtils.isNotBlank(startTimeStr)) {
                String endTimeStr = pluginJobConf.getString("endTime");
                this.startTime = parseFlexibleInstant(startTimeStr);
                this.endTime = (StringUtils.isNotBlank(endTimeStr)) ? parseFlexibleInstant(endTimeStr) : Instant.now();

                this.windowSizeMs = pluginJobConf.getLong("windowSizeMs", 60000L);
                this.pageLimit = pluginJobConf.getInt("pageLimit", 1000);

                LOG.info("InfluxDB Reader启用分批采集：measurement={} startTime={} endTime={} windowSizeMs={} pageLimit={}",
                        measurement, startTime, endTime, windowSizeMs, pageLimit);
            } else {
                this.customSql = String.format("SELECT %s FROM \"%s\"", String.join(",", columns), measurement);
            }

        } else {
            if (columns == null || columns.isEmpty()) {
                throw new IllegalArgumentException("自定义语句查询时，columns不能为空");
            }
        }
        LOG.info("influxdb query: {}", customSql);
    }

    public static long toNanos(Instant instant) {
        return instant.getEpochSecond() * 1_000_000_000L + instant.getNano();
    }

    private Instant parseFlexibleInstant(String timeStr) {
        List<DateTimeFormatter> formatters = Arrays.asList(
                DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault()),
                DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.systemDefault()),
                DateTimeFormatter.ISO_LOCAL_DATE_TIME.withZone(ZoneId.systemDefault()),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault()),
                DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss").withZone(ZoneId.systemDefault()),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneId.systemDefault())
        );

        for (DateTimeFormatter formatter : formatters) {
            try {
                return formatter.parse(timeStr, Instant::from);
            } catch (Exception ignored) {
                // 忽略尝试失败
            }
        }
        throw new RuntimeException("无法解析时间字符串: " + timeStr);
    }

    @Override
    public void prepare() {
        LOG.info("influxdb ping...");
        Pong ping = influxDB.ping();
        LOG.info("influxdb pong: {}", ping);
    }

    @Override
    public void startRead(RecordSender recordSender) {
        if (StringUtils.isNotBlank(customSql)) {
            // 自定义SQL模式
            executeQueryAndSend(customSql, recordSender);
        } else {
            // 按时间窗口分页拉取模式
            Instant windowStart = this.startTime;
            while (true) {
                Instant windowEnd = windowStart.plusMillis(windowSizeMs);
                Instant actualEnd = (endTime != null) ? endTime : Instant.now();
                if (windowStart.isAfter(actualEnd) || windowStart.equals(actualEnd)) {
                    break;
                }
                if (windowEnd.isAfter(actualEnd)) {
                    windowEnd = actualEnd;
                }

                LOG.info("拉取窗口数据: {} ~ {}", windowStart, windowEnd);
                long windowRowCount = 0;
                int offset = 0;
                while (true) {
                    String querySql = String.format(
                            "SELECT %s FROM \"%s\" WHERE time >= '%s' AND time < '%s' LIMIT %d OFFSET %d",
                            String.join(",", columns),
                            measurement,
                            windowStart,
                            windowEnd,
                            pageLimit,
                            offset
                    );

                    LOG.info("执行sql:{}", querySql);
                    long pageCount = executeQueryAndSend(querySql, recordSender);
                    windowRowCount += pageCount;
                    LOG.info("窗口 {} ~ {}，分页 offset={}，采集条数: {}", windowStart, windowEnd, offset, pageCount);

                    if (pageCount < pageLimit) {
                        break;
                    }
                    offset += pageLimit;
                }
                LOG.info("窗口 {} ~ {} 总采集条数: {}", windowStart, windowEnd, windowRowCount);
                windowStart = windowEnd;
            }
        }
    }

    private long executeQueryAndSend(String querySql, RecordSender recordSender) {
        Query query = new Query(querySql, dataSource.getDatabase());
        QueryResult queryResult = influxDB.query(query);

        if (queryResult.hasError()) {
            throw new RuntimeException("InfluxDB 查询错误: " + queryResult.getError());
        }

        long dataCount = 0;
        for (QueryResult.Result result : queryResult.getResults()) {
            if (result.getSeries() != null) {
                for (QueryResult.Series series : result.getSeries()) {
                    List<String> seriesColumns = series.getColumns();
                    List<List<Object>> values = series.getValues();
                    for (List<Object> row : values) {
                        Record record = buildRecord(recordSender, seriesColumns, series.getTags(), row);
                        recordSender.sendToWriter(record);
                        dataCount++;
                    }
                }
            }
        }
        return dataCount;
    }

    protected Record buildRecord(RecordSender recordSender, List<String> columns, Map<String, String> tags, List<Object> row) {
        Record record = recordSender.createRecord();
        for (String column : this.columns) {
            int columnIndex = columns.indexOf(column);
            if (columnIndex != -1) {
                Object value = row.get(columnIndex);
                record.addColumn(convertToColumn(value));
            } else if (tags != null && tags.containsKey(column)) {
                record.addColumn(new StringColumn(tags.get(column)));
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
