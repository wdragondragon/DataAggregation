package com.jdragon.aggregation.datasource.queue.rocketmq;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.queue.QueueAbstract;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

@Slf4j
public class RocketQueue extends QueueAbstract {

    private DefaultMQProducer producer;

    String namesrvAddr;

    String topic;

    String tag;

    String producerGroup;

    String accessKey;

    String secretKey;

    String consumerGroup;

    Integer pullBatchSize;

    Long pullInterval;

    public RocketQueue() {

    }

    @Override
    public void init() {
        // 从 Map 中提取参数
        Configuration configParams = getPluginQueueConf();
        namesrvAddr = (String) configParams.get("namesrvAddr");
        producerGroup = (String) configParams.get("producerGroup");
        accessKey = (String) configParams.get("accessKey");
        secretKey = (String) configParams.get("secretKey");
        topic = configParams.getString("topic");
        tag = configParams.getString("tag");
        consumerGroup = configParams.getString("consumerGroup");
        pullBatchSize = configParams.getInt("pullBatchSize", 100);
        pullInterval = configParams.getLong("pullInterval", -1);
    }

    @Override
    public void sendMessage(String message) throws Exception {
        if (producer == null) {
            if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey)) {
                AclClientRPCHook auth = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
                producer = new DefaultMQProducer(producerGroup, auth);
            } else {
                producer = new DefaultMQProducer(producerGroup);
            }
            producer.setNamesrvAddr(namesrvAddr);
            try {
                producer.start();
            } catch (MQClientException e) {
                throw new RuntimeException(e);
            }
        }
        if (StringUtils.isBlank(tag)) {
            tag = null;
        }
        Message msg = new Message(topic, tag, message.getBytes());
        log.debug("RocketMQ发送消息: {}", message);
        SendResult sendResult = producer.send(msg);
        log.debug("RocketMQ消息已发送: {}", sendResult);
    }

    @Override
    public void receiveMessage(Function<String, Boolean> messageProcessor) throws Exception {
        // RocketMQ 消费者实现
        // 创建消费者实例
        String subExpression = StringUtils.isBlank(tag) ? "*" : tag;
        if (StringUtils.isBlank(subExpression)) {
            subExpression = null;
        }
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer();
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.subscribe(topic, subExpression);
        consumer.setPullBatchSize(pullBatchSize);
        consumer.setAutoCommit(false);
        consumer.setConsumerGroup(consumerGroup);
        // 启动消费者
        consumer.start();

        while (true) {
            List<MessageExt> poll = consumer.poll(1000);
            AtomicBoolean sign = new AtomicBoolean(true);
            poll.forEach(record -> {
                String message = record.toString();
                boolean continueConsume = messageProcessor.apply(message);
                if (!continueConsume) {
                    log.info("停止消费消息: {}", message);
                    sign.set(false);
                    return;
                }
                log.debug("Kafka 消费消息: {}", message);
            });
            if (!poll.isEmpty()) {
                consumer.commitSync();
            }
            if (!sign.get()) {
                break;
            }
            if (pullBatchSize != -1) {
                Thread.sleep(pullBatchSize);
            }
        }
        consumer.shutdown();
    }

    @Override
    public void destroy() {
        if (producer != null) {
            producer.shutdown();
        }
    }
}
