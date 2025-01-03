package com.jdragon.aggregation.datasource.queue;

import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;

import java.util.Map;
import java.util.function.Function;

public abstract class QueueAbstract extends AbstractPlugin {

    protected Map<String, Object> configParams;

    // 构造方法，接收 Map 类型的参数
    public QueueAbstract() {
    }

    // 抽象的初始化方法，由子类实现队列初始化逻辑
    public abstract void init(Map<String, Object> config) throws Exception;

    // 发送消息的抽象方法
    public abstract void sendMessage(String message) throws Exception;

    // 接收消息的抽象方法
    // 接收消息的抽象方法
    // 传入一个 Function 来处理每条消息，并根据返回值决定是否继续消费
    public abstract void receiveMessage(Function<String, Boolean> messageProcessor) throws Exception;

    // 关闭资源的抽象方法
    public abstract void close() throws Exception;
}
