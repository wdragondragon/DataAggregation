package com.jdragon.aggregation.core.job.pipline.asyn;

import com.jdragon.aggregation.core.job.Message;

public class TransformerExec extends StreamHandler {
    private final TransformerFunction[] functionArray;

    public TransformerExec(TransformerFunction... functionArray) {
        this.functionArray = functionArray;
    }

    @Override
    public void process() throws InterruptedException {
        while (true) {
            Message message = super.take();  // 从前一个节点获取消息
            for (TransformerFunction function : functionArray) {
                message = function.apply(message);  // 转换消息
            }
            super.put(message);  // 将处理后的消息推送到下一个节点
        }
    }

    @FunctionalInterface
    public interface TransformerFunction {
        Message apply(Message message);
    }
}
