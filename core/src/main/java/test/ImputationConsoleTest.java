package test;

import com.jdragon.aggregation.commons.element.*;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.job.JobContainer;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 数据补全控制台测试
 * 使用dx_imputation transformer和console writer
 */
public class ImputationConsoleTest {
    
    private final static Logger log = LoggerFactory.getLogger(ImputationConsoleTest.class);
    
    public static void main(String[] args) {
        // 加载配置文件
        Configuration configuration = Configuration.from(new File("C:\\dev\\ideaProject\\DataAggregation\\core\\src\\main\\resources\\imputation_console.json"));
        
        JobContainer container = new JobContainer(configuration);
        
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
        log.info("启动数据补全控制台测试作业...");
        container.start();
        log.info("作业执行完成");
    }
}