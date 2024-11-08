package com.jdragon.aggregation.core.job.simple;

import com.jdragon.aggregation.core.job.Message;

public class UpperCaseTransformer implements Transformer {

    @Override
    public Message transform(Message message) {
        String transformedContent = message.getContent().toUpperCase();
        return new Message(transformedContent);
    }
}
