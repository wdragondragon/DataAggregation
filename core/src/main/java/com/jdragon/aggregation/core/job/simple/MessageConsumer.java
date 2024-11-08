package com.jdragon.aggregation.core.job.simple;

import com.jdragon.aggregation.core.job.Message;

import java.util.function.Consumer;

public class MessageConsumer implements Consumer<Message> {

    @Override
    public void accept(Message message) {
        System.out.println("Consumed: " + message.getContent());
    }
}
