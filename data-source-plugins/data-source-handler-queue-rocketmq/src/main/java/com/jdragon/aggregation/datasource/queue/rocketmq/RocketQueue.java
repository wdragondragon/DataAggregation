package com.jdragon.aggregation.datasource.queue.rocketmq;

import com.jdragon.aggregation.datasource.queue.QueueAbstract;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Map;
import java.util.function.Function;

@Slf4j
public class RocketQueue extends QueueAbstract {

    private DefaultMQProducer producer;

    private DefaultMQPushConsumer consumer;

    public RocketQueue() {

    }

    @Override
    public void init(Map<String, Object> config) throws Exception {
        // 从 Map 中提取参数
        super.configParams = config;
        String namesrvAddr = (String) configParams.get("namesrvAddr");
        String producerGroup = (String) configParams.get("producerGroup");
        String accessKey = (String) configParams.get("accessKey");
        String secretKey = (String) configParams.get("secretKey");
        AclClientRPCHook auth = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
        producer = new DefaultMQProducer(producerGroup, auth);
        producer.setNamesrvAddr(namesrvAddr);
        try {
            producer.start();
        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void sendMessage(String message) throws Exception {
        String topic = (String) configParams.get("topic");
        String tag = (String) configParams.get("tag");
        if (StringUtils.isBlank(tag)) {
            tag = null;
        }
        Message msg = new Message(topic, tag, message.getBytes());
        log.info("RocketMQ发送消息: {}", message);
        SendResult sendResult = producer.send(msg);
        log.info("RocketMQ消息已发送: {}", sendResult);
    }

    @Override
    public void receiveMessage(Function<String, Boolean> messageProcessor) throws Exception {
        // RocketMQ 消费者实现
        // 创建消费者实例
        String consumerGroup = (String) configParams.get("consumerGroup");
        String namesrvAddr = (String) configParams.get("namesrvAddr");
        String topic = (String) configParams.get("topic");
        String subExpression = (String) configParams.getOrDefault("tag", "*");
        if (StringUtils.isBlank(subExpression)) {
            subExpression = null;
        }
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(namesrvAddr);
        // 订阅主题
        consumer.subscribe(topic, subExpression);

        // 注册消息监听
        consumer.registerMessageListener((MessageListenerOrderly) (msgList, context) -> {
            for (MessageExt msg : msgList) {
                String message = new String(msg.getBody());
                Boolean apply = messageProcessor.apply(message);
                if (!apply) {
                    log.info("停止消费消息: {}", message);
                    consumer.resume();
                    break;
                }
                log.info("收到消息：{}", message);
            }
            return ConsumeOrderlyStatus.SUCCESS;
        });
        // 启动消费者
        consumer.start();
    }

    public void close() {
        if (producer != null) {
            producer.shutdown();
        }
        if (consumer != null) {
            consumer.shutdown();
        }
    }
}
