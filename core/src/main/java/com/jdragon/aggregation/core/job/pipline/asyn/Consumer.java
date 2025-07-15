package com.jdragon.aggregation.core.job.pipline.asyn;


import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.transport.record.TerminateRecord;

public class Consumer extends StreamHandler {
    private final java.util.function.Consumer<Record> consumerFunction;

    public Consumer(java.util.function.Consumer<Record> consumerFunction) {
        this.consumerFunction = consumerFunction;
    }

    @Override
    public void process() throws InterruptedException {
        while (true) {
            Record message = super.take();  // 从前一个节点获取消息
            if (message instanceof TerminateRecord) {
                break;
            }
            consumerFunction.accept(message);  // 消费消息
        }
    }
}
