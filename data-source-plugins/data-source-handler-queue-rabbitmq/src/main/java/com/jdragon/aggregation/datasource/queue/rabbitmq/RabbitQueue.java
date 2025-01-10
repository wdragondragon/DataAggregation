package com.jdragon.aggregation.datasource.queue.rabbitmq;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.queue.QueueAbstract;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

@Slf4j
public class RabbitQueue extends QueueAbstract {

    private Connection connection;

    private Channel channel;

    private String queueName;

    public RabbitQueue() {

    }

    @Override
    public void init() {
        // 从 Map 中提取参数
        Configuration configParams = getPluginQueueConf();
        queueName = (String) configParams.get("queueName");
        String host = configParams.getString("host", "localhost");
        String username = configParams.getString("username", "guest");
        String password = configParams.getString("password", "guest");
        Integer port = configParams.getInt("port", 5672);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setPort(port);
        try {
            connection = factory.newConnection();
            channel = connection.createChannel();
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
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
    public void destroy() {
        try {
            if (channel != null) {
                channel.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }
}
