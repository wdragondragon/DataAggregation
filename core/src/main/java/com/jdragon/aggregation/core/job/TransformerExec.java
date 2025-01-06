package com.jdragon.aggregation.core.job;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.plugin.Transformer;

public class TransformerExec extends StreamHandler {
    private final Transformer[] functionArray;

    public TransformerExec(Transformer... functionArray) {
        this.functionArray = functionArray;
    }

    @Override
    public void process() throws InterruptedException {
        while (true) {
            Record message = super.take();  // 从前一个节点获取消息
            for (Transformer function : functionArray) {
                message = function.evaluate(message);  // 转换消息
            }
            super.put(message);  // 将处理后的消息推送到下一个节点
        }
    }
}
