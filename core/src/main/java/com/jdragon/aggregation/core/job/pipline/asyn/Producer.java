package com.jdragon.aggregation.core.job.pipline.asyn;

import com.jdragon.aggregation.core.job.Message;

import java.util.function.Supplier;

public class Producer extends StreamHandler {
    private final Supplier<String> dataSupplier;

    public Producer(Supplier<String> dataSupplier) {
        this.dataSupplier = dataSupplier;
    }


    @Override
    public void process() throws InterruptedException {
        while (true) {
            String data = dataSupplier.get();
            if (data != null) {
                Message message = new Message(data);
                super.put(message);  // 生产数据并推送到下一个节点
            }
        }
    }
}
