package test;

import com.jdragon.aggregation.commons.element.*;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.job.JobContainer;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 自定义数据补全测试
 * 模拟读取包含空字段的数据，使用自定义transformer进行补全
 */
public class CustomImputationTest {

    private final static Logger log = LoggerFactory.getLogger(CustomImputationTest.class);

    public static void main(String[] args) {
        // 加载配置文件
        Configuration configuration = Configuration.from(new File("C:\\dev\\ideaProject\\DataAggregation\\core\\src\\main\\resources\\custom-imputation.json"));

        JobContainer container = new JobContainer(configuration);


        container.addConsumerTransformer(new Transformer() {
            {
                setTransformerName("custom_imputation");
            }

            @Override
            public Record evaluate(Record record, Object... paras) {
                if (paras == null || paras.length < 2) {
                    log.warn("transformer参数不足，跳过处理");
                    return record;
                }

                try {
                    // 参数结构：paras[0]是columnIndex，paras[1]是strategy，paras[2]是可选值
                    int columnIndex;
                    String strategy;
                    String defaultValue = null;

                    if (paras[0] instanceof Integer) {
                        columnIndex = (Integer) paras[0];
                    } else if (paras[0] instanceof String) {
                        columnIndex = Integer.parseInt((String) paras[0]);
                    } else {
                        log.warn("无效的columnIndex参数类型: {}", paras[0].getClass());
                        return record;
                    }

                    if (paras.length > 1) {
                        strategy = (String) paras[1];
                    } else {
                        log.warn("缺少strategy参数");
                        return record;
                    }

                    if (paras.length > 2) {
                        defaultValue = (String) paras[2];
                    }

                    Column column = record.getColumn(columnIndex);

                    // 检查是否为空
                    if (column.getRawData() == null) {
                        log.info("检测到空字段，列索引: {}, 策略: {}, 默认值: {}", columnIndex, strategy, defaultValue);

                        Column filledColumn = null;
                        switch (strategy.toLowerCase()) {
                            case "constant":
                                if (defaultValue != null) {
                                    // 根据列索引尝试确定类型
                                    filledColumn = new StringColumn(defaultValue);
                                }
                                break;
                            case "mean":
                                // 简单实现：使用默认值作为均值
                                if (defaultValue != null) {
                                    try {
                                        double meanValue = Double.parseDouble(defaultValue);
                                        filledColumn = new DoubleColumn(meanValue);
                                    } catch (NumberFormatException e) {
                                        filledColumn = new DoubleColumn(0.0);
                                    }
                                }
                                break;
                            case "forward_fill":
                                // 前向填充 - 这里简化实现，使用默认值
                                log.warn("forward_fill策略简化实现，使用默认值");
                                if (defaultValue != null) {
                                    double doubleValue = Double.parseDouble(defaultValue);
                                    filledColumn = new DoubleColumn(doubleValue);
                                }
                                break;
                        }

                        if (filledColumn != null) {
                            record.setColumn(columnIndex, filledColumn);
                            log.info("字段补全完成: 列 {} -> {}", columnIndex, filledColumn.asString());
                        }
                    }

                    return record;
                } catch (Exception e) {
                    log.error("transformer处理异常", e);
                    return record;
                }
            }
        });

        // 注册自定义reader
        container.addConsumerPlugin(PluginType.READER, (config, peerConfig) -> new Reader.Job() {
            private int recordCount = 0;
            private final int maxRecords = 10; // 生成10条测试记录

            @Override
            public void init() {
                log.info("自定义reader初始化 - 生成包含空字段的测试数据");
            }

            @Override
            public void startRead(RecordSender recordSender) {
                log.info("开始读取数据...");

                // 生成测试数据，包含空字段
                while (recordCount < maxRecords) {
                    Record record = new DefaultRecord();

                    // id列 - 非空
                    record.setColumn(0, new LongColumn(recordCount + 1));

                    // name列 - 偶数行为空
                    if (recordCount % 2 == 0) {
                        record.setColumn(1, new StringColumn("User" + (recordCount + 1)));
                    } else {
                        // 创建空列
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

        // 启动作业
        log.info("启动自定义数据补全作业...");
        container.start();
        log.info("作业执行完成");
    }
}