package com.jdragon.aggregation.datasource.file.tbds.hdfs;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 通用 Spark Parquet 文件读取工具
 * 支持 HDFS，自动并行分区执行
 */
@Slf4j
public class SparkParquetReader {


    public static SparkSession createSparkSession(String master, Map<String, String> config) {
        log.info("创建spark session:[{}], config:[{}]", master, JSONObject.toJSONString(config));
        SparkSession.Builder builder = SparkSession.builder()
                .appName("SparkParquetReader");
        builder.master(master);
        config.forEach(builder::config);
        return builder.getOrCreate();
    }


    /**
     * 从 HDFS 读取 Parquet 文件并分布式回调插件接口
     *
     * @param spark     SparkSession 实例
     * @param hdfsPath  Parquet 文件路径或目录
     * @param rowHandler 插件回调（分布式执行）
     */
    public static void readParquetDistributed(SparkSession spark, String hdfsPath, Map<String, String> option, Consumer<Map<String, Object>> rowHandler) {
        log.info("正在读取Parquet文件: {}, option：{}", hdfsPath, JSONObject.toJSONString(option));
        DataFrameReader sparkRead = spark.read();
        option.forEach(sparkRead::option);
        Dataset<Row> df = sparkRead.parquet(hdfsPath);

        displayDataInfo(df);

        StructType schema = df.schema();

        df.foreachPartition((ForeachPartitionFunction<Row>) iterator ->
                processPartition(iterator, schema, rowHandler));
    }

    /**
     * 在 Executor 分区上处理数据
     */
    private static void processPartition(Iterator<Row> iterator, StructType schema, Consumer<Map<String, Object>> rowHandler) {
        StructField[] fields = schema.fields();

        while (iterator.hasNext()) {
            Row row = iterator.next();
            Map<String, Object> map = new HashMap<>();
            for (StructField field : fields) {
                map.put(field.name(), row.getAs(field.name()));
            }
            rowHandler.accept(map);
        }
    }

    private static void displayDataInfo(Dataset<Row> df) {
        log.info("数据Schema:");
        df.printSchema();

        long totalRecords = df.count();
        log.info("总记录数: {}", totalRecords);

        log.info("数据预览（前5条）:");
        df.show(5, false);
    }
}
