package test;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.datasource.queue.QueueAbstract;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RabbitTest {
    public static void main(String[] args) throws Exception {
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, "rabbitmq")) {
            QueueAbstract queueAbstract = classLoaderSwapper.loadPlugin();
            Configuration rabbitMqParam = Configuration.newDefault();
            rabbitMqParam.set("host", "192.168.100.194");
            rabbitMqParam.set("port", 5672);
            rabbitMqParam.set("username", "guest");
            rabbitMqParam.set("password", "guest");
            rabbitMqParam.set("queueName", "rabbitTest");
            queueAbstract.setPluginQueueConf(rabbitMqParam);
            queueAbstract.init();
            int i = 0;
            while (i++ < 10) {
                queueAbstract.sendMessage("test" + i);
            }
            queueAbstract.sendMessage("exit");
            queueAbstract.receiveMessage(message -> {
                log.info("处理 rabbitmq 消息: {}", message);
                return !message.equals("exit");  // 如果收到 exit 消息，停止消费
            });
            queueAbstract.destroy();
        }
    }
}
