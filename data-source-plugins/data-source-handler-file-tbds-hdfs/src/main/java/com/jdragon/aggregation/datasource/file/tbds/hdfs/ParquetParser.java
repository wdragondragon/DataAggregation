package com.jdragon.aggregation.datasource.file.tbds.hdfs;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ParquetParser {
    public static void main(String[] args) {
        String filePath = "C:\\Users\\jdrag\\Desktop\\part-00002-c191074a-c7a7-4170-8839-c9fe5ede209d-c000.snappy.parquet";

        // 创建配置对象并启用INT96兼容模式
        Configuration conf = new Configuration();
        conf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true); // 关键修复

        try (ParquetReader<GenericRecord> reader = AvroParquetReader
                .<GenericRecord>builder(new Path(filePath))
                .withConf(conf) // 使用自定义配置
                .build()) {

            GenericRecord record;
            while ((record = reader.read()) != null) {
                // 处理记录
                GenericRecord finalRecord = record;
                record.getSchema().getFields().forEach(field -> {
                    String fieldName = field.name();
                    Object value = finalRecord.get(fieldName);

                    // 特殊处理INT96字段（通常是时间戳）
                    if ("data_time".equals(fieldName) || "insert_time".equals(fieldName)) { // 替换为你的实际字段名
                        byte[] bytes = null;

                        // 检查不同的可能类型
                        if (value instanceof org.apache.avro.generic.GenericData.Fixed) {
                            // 当readInt96AsFixed=true时，INT96被转换为Fixed类型
                            org.apache.avro.generic.GenericData.Fixed fixed = (GenericData.Fixed) value;
                            bytes = fixed.bytes();
                        } else if (value instanceof ByteBuffer) {
                            // 兼容ByteBuffer类型
                            ByteBuffer buffer = (ByteBuffer) value;
                            bytes = buffer.array();
                        }
                        try {
                            LocalDateTime dt = Int96Converter.convertInt96ToDateTime(bytes);
                            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
                            System.out.println(fieldName+": " + dt.format(formatter));
                        } catch (Exception e) {
                            System.err.println("Error converting timestamp: " + e.getMessage());
                            System.out.println(fieldName + ": [INVALID TIMESTAMP]");
                        }
                    } else {
                        System.out.println(fieldName + ": " + value);
                    }
                });
                System.out.println("----------------------");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

