//package com.jdragon.aggregation.datasource.file.tbds.hdfs;
//
//import org.apache.spark.sql.DataFrameWriter;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SaveMode;
//
//import java.util.Map;
//import java.util.Properties;
//
//public class SparkJdbcWriter {
//    public static void writeToJdbc(Dataset<Row> parquetDF, String jdbcUrl, String table, Properties connectionProperties, Map<String, String> option) {
//        // 5. 写入MySQL（带性能优化参数）
//        DataFrameWriter<Row> dataFrameWriter = parquetDF.write()
//                .mode(SaveMode.Append);
//        option.forEach(dataFrameWriter::option);
//        dataFrameWriter.jdbc(jdbcUrl, table, connectionProperties);
//    }
//}
