package com.jdragon.aggregation.core.job.pipline.asyn;


import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.transport.record.TerminateRecord;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TransformerExec extends StreamHandler {
    private final TransformerFunction[] functionArray;

    public TransformerExec(TransformerFunction... functionArray) {
        this.functionArray = functionArray;
    }

    @Override
    public void process() throws InterruptedException {
        while (true) {
            Record message = super.take();  // 从前一个节点获取消息
            if (message == null || message instanceof TerminateRecord) {
                super.put(message);  // 将处理后的消息推送到下一个节点
                break;
            }
            for (TransformerFunction function : functionArray) {
                message = function.apply(message);  // 转换消息
            }
            super.put(message);  // 将处理后的消息推送到下一个节点
        }
        end();
        log.info("transformer close run status:{}", isRunning());
    }

    @FunctionalInterface
    public interface TransformerFunction {
        Record apply(Record message);
    }
}
