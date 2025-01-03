package com.jdragon.aggregation.core;

import com.jdragon.aggregation.datasource.queue.QueueAbstract;
import com.jdragon.aggregation.datasource.queue.rocketmq.RocketQueue;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class RocketTest {
    public static void main(String[] args) throws Exception {
        QueueAbstract queueAbstract = new RocketQueue();
        Map<String, Object> params = new HashMap<>();
        params.put("namesrvAddr", "192.168.100.194:9876");
        params.put("topic", "TestTopic");
        params.put("consumerGroup", "please_rename_unique_group_name");
        params.put("producerGroup", "please_rename_unique_group_name");
        params.put("subExpression", "*");
        queueAbstract.init(params);
        queueAbstract.receiveMessage(message -> {
            log.info("处理 rocket 消息: {}", message);
            return !message.equals("exit");  // 如果收到 exit 消息，停止消费
        });
        int i = 0;
        while (i++ < 10) {
            queueAbstract.sendMessage("test" + i);
        }
        queueAbstract.sendMessage("exit");

        queueAbstract.close();
    }
}
