package com.jdragon.aggregation.datasource.queue.rabbitmq;

import com.jdragon.aggregation.datasource.queue.QueueAbstract;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.function.Function;

@Slf4j
public class RabbitQueue extends QueueAbstract {

    private ConnectionFactory factory;

    private Connection connection;

    private Channel channel;

    private String queueName;

    public RabbitQueue() {

    }

    @Override
    public void init(Map<String, Object> config) throws Exception {
        // 从 Map 中提取参数
        super.configParams = config;

        queueName = (String) configParams.get("queueName");
        String host = (String) configParams.getOrDefault("host", "localhost");
        String username = (String) configParams.getOrDefault("username", "guest");
        String password = (String) configParams.getOrDefault("password", "guest");
        Integer port = (Integer) configParams.getOrDefault("port", 5672);

        factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setPort(port);
        connection = factory.newConnection();
        channel = connection.createChannel();
    }

    @Override
    public void sendMessage(String message) throws Exception {
        channel.queueDeclare(queueName, false, false, false, null);
        channel.basicPublish("", queueName, null, message.getBytes(StandardCharsets.UTF_8));
        log.info("RabbitMQ消息已发送: {}", message);
    }

    @Override
    public void receiveMessage(Function<String, Boolean> messageProcessor) throws Exception {
        channel.queueDeclare(queueName, false, false, false, null);
        log.info("等待接收RabbitMQ消息...");

        // 持续消费消息，直到 messageProcessor 返回 false
        while (true) {
            GetResponse response = channel.basicGet(queueName, true);
            if (response != null) {
                String message = new String(response.getBody(), StandardCharsets.UTF_8);
                boolean continueConsume = messageProcessor.apply(message);
                if (!continueConsume) {
                    log.info("停止消费消息: {}", message);
                    break;
                }
                log.info("RabbitMQ 消费消息: {}", message);
            }
        }
    }


    @Override
    public void close() throws Exception {
        if (channel != null) {
            channel.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
