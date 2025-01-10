package com.jdragon.aggregation.datasource.queue;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import lombok.Getter;
import lombok.Setter;

import java.util.function.Function;

@Setter
@Getter
public abstract class QueueAbstract extends AbstractPlugin {

    protected Configuration pluginQueueConf;

    // 构造方法，接收 Map 类型的参数
    public QueueAbstract() {
    }

    // 发送消息的抽象方法
    public abstract void sendMessage(String message) throws Exception;

    // 接收消息的抽象方法
    // 接收消息的抽象方法
    // 传入一个 Function 来处理每条消息，并根据返回值决定是否继续消费
    public abstract void receiveMessage(Function<String, Boolean> messageProcessor) throws Exception;
}
