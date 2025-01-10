package test;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.datasource.queue.QueueAbstract;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RocketTest {
    public static void main(String[] args) throws Exception {
        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, "rocketmq")) {
            QueueAbstract queueAbstract = classLoaderSwapper.loadPlugin();
            Configuration params = Configuration.newDefault();
            params.set("namesrvAddr", "192.168.100.194:9876");
            params.set("topic", "TestTopic");
            params.set("consumerGroup", "please_rename_unique_group_name");
            params.set("producerGroup", "please_rename_unique_group_name");
            params.set("subExpression", "*");
            queueAbstract.setPluginQueueConf(params);
            queueAbstract.init();
            queueAbstract.receiveMessage(message -> {
                log.info("处理 rocket 消息: {}", message);
                return !message.equals("exit");  // 如果收到 exit 消息，停止消费
            });
            int i = 0;
            while (i++ < 10) {
                queueAbstract.sendMessage("test" + i);
            }
            queueAbstract.sendMessage("exit");

            queueAbstract.destroy();
        }
    }
}
