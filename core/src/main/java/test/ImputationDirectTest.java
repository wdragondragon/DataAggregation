package test;

import com.jdragon.aggregation.commons.element.*;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.job.JobContainer;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.RecordReceiver;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 直接测试dx_imputation transformer
 * 使用自定义reader和writer，避免插件加载问题
 */
public class ImputationDirectTest {
    private static final Logger log = LoggerFactory.getLogger(ImputationDirectTest.class);

    public static void main(String[] args) {
        // 创建基础配置
        Configuration configuration = Configuration.newDefault();
        long jobId = 1;

        // 设置reader和writer为custom类型
        Configuration custom = Configuration.from(new HashMap<String, Object>() {{
            put("type", "custom");
            put("config", new HashMap<>());
        }});

        configuration.set("jobId", jobId);
        configuration.set("reader", custom);
        configuration.set("writer", custom);
        
        // 添加transformer配置
        List<Map<String, Object>> transformers = new ArrayList<>();
        
        // transformer 1: 补全name列 (列索引1) 使用constant策略
        Map<String, Object> transformer1 = new HashMap<>();
        transformer1.put("name", "dx_imputation");
        Map<String, Object> param1 = new HashMap<>();
        param1.put("columnIndex", 1);
        param1.put("paras", new String[]{"constant", "Unknown"});
        transformer1.put("parameter", param1);
        transformers.add(transformer1);
        
        // transformer 2: 补全age列 (列索引2) 使用constant策略
        Map<String, Object> transformer2 = new HashMap<>();
        transformer2.put("name", "dx_imputation");
        Map<String, Object> param2 = new HashMap<>();
        param2.put("columnIndex", 2);
        param2.put("paras", new String[]{"constant", "25"});
        transformer2.put("parameter", param2);
        transformers.add(transformer2);
        
        // transformer 3: 补全salary列 (列索引3) 使用mean策略
        Map<String, Object> transformer3 = new HashMap<>();
        transformer3.put("name", "dx_imputation");
        Map<String, Object> param3 = new HashMap<>();
        param3.put("columnIndex", 3);
        param3.put("paras", new String[]{"mean", "5500.0"});
        transformer3.put("parameter", param3);
        transformers.add(transformer3);
        
        // transformer 4: 补全department列 (列索引4) 使用constant策略
        Map<String, Object> transformer4 = new HashMap<>();
        transformer4.put("name", "dx_imputation");
        Map<String, Object> param4 = new HashMap<>();
        param4.put("columnIndex", 4);
        param4.put("paras", new String[]{"constant", "DefaultDept"});
        transformer4.put("parameter", param4);
        transformers.add(transformer4);
        
        configuration.set("transformer", transformers);

        JobContainer container = new JobContainer(configuration);
        
        // 注册自定义reader - 生成包含空字段的测试数据
        container.addConsumerPlugin(PluginType.READER, (config, peerConfig) -> new Reader.Job() {
            private int recordCount = 0;
            private final int maxRecords = 10;

            @Override
            public void init() {
                log.info("自定义reader初始化 - 生成包含空字段的测试数据");
            }

            @Override
            public void startRead(RecordSender recordSender) {
                log.info("开始读取数据...");

                while (recordCount < maxRecords) {
                    Record record = new DefaultRecord();

                    // id列 - 非空
                    record.setColumn(0, new LongColumn(recordCount + 1));

                    // name列 - 偶数行为空
                    if (recordCount % 2 == 0) {
                        record.setColumn(1, new StringColumn("User" + (recordCount + 1)));
                    } else {
                        record.setColumn(1, new StringColumn(null));
                    }

                    // age列 - 第3、6、9行为空
                    if (recordCount % 3 == 2) {
                        record.setColumn(2, new LongColumn((Long) null));
                    } else {
                        record.setColumn(2, new LongColumn(20 + recordCount));
                    }

                    // salary列 - 第5行为空
                    if (recordCount == 4) {
                        record.setColumn(3, new DoubleColumn((Double) null));
                    } else {
                        record.setColumn(3, new DoubleColumn(5000.0 + recordCount * 100));
                    }

                    // department列 - 第7、8行为空
                    if (recordCount == 6 || recordCount == 7) {
                        record.setColumn(4, new StringColumn(null));
                    } else {
                        record.setColumn(4, new StringColumn("Dept" + ((recordCount % 3) + 1)));
                    }

                    recordSender.sendToWriter(record);
                    recordCount++;

                    log.info("生成记录 {}: id={}, name={}, age={}, salary={}, department={}",
                            recordCount,
                            record.getColumn(0).asLong(),
                            record.getColumn(1).asString(),
                            record.getColumn(2).asLong(),
                            record.getColumn(3).asDouble(),
                            record.getColumn(4).asString());
                }

                log.info("数据生成完成，共生成 {} 条记录", maxRecords);
            }

            @Override
            public void post() {
                log.info("自定义reader清理完成");
            }
        });

        // 注册自定义writer - 简单打印记录
        container.addConsumerPlugin(PluginType.WRITER, (config, peerConfig) -> new Writer.Job() {
            @Override
            public void init() {
                log.info("自定义writer初始化");
            }

            @Override
            public void startWrite(RecordReceiver recordReceiver) {
                log.info("开始写入数据...");
                Record record;
                int count = 0;
                while ((record = recordReceiver.getFromReader()) != null) {
                    count++;
                    // 打印补全后的记录
                    log.info("处理后记录 {}: id={}, name={}, age={}, salary={}, department={}",
                            count,
                            record.getColumn(0).asLong(),
                            record.getColumn(1).asString(),
                            record.getColumn(2).asLong(),
                            record.getColumn(3).asDouble(),
                            record.getColumn(4).asString());
                }
                log.info("数据处理完成，共处理 {} 条记录", count);
            }

            @Override
            public void post() {
                log.info("自定义writer清理完成");
            }
        });

        // 启动作业
        log.info("启动直接数据补全测试作业...");
        container.start();
        log.info("作业执行完成");
    }
}