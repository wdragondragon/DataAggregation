package com.jdragon.aggregation.core.job.simple;

import com.jdragon.aggregation.core.job.Message;

public interface Transformer {
    Message transform(Message message);
}
