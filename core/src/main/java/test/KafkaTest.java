package test;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.datasource.queue.QueueAbstract;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaTest {
    public static void main(String[] args) throws Exception {
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, "kafka")) {
            QueueAbstract kafkaQueue = classLoaderSwapper.loadPlugin();
            // 创建 Kafka 队列配置参数
            Configuration kafkaParams = Configuration.newDefault();
            kafkaParams.set("bootstrap.servers", "192.168.100.194:9092");
            kafkaParams.set("group.id", "test-consumer-group");
            kafkaParams.set("topic", "quickstart-events");
            kafkaParams.set("auto.offset.reset", "latest");
            kafkaQueue.setPluginQueueConf(kafkaParams);
            kafkaQueue.init();
            int i = 0;
            while (i++ < 10) {
                kafkaQueue.sendMessage("test" + i);
            }
            kafkaQueue.sendMessage("exit");
            kafkaQueue.receiveMessage(message -> {
                log.info("处理 Kafka 消息: {}", message);
                return !message.equals("exit");  // 如果收到 exit 消息，停止消费
            });
            kafkaQueue.destroy();
        }
    }
}
