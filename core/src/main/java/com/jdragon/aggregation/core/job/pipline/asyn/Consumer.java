package com.jdragon.aggregation.core.job.pipline.asyn;


import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.plugin.spi.Writer;
import com.jdragon.aggregation.core.transport.exchanger.BufferedRecordExchanger;
import com.jdragon.aggregation.core.transport.record.TerminateRecord;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Consumer extends StreamHandler {
    private java.util.function.Consumer<Record> consumerFunction;
    private Writer.Job writerJob;

    public Consumer(java.util.function.Consumer<Record> consumerFunction) {
        this.consumerFunction = consumerFunction;
    }

    public Consumer(Writer.Job writeJob) {
        this.writerJob = writeJob;
    }

    @Override
    public void process() throws InterruptedException {
        writerJob.startWrite(new BufferedRecordExchanger(getInputQueue()));
        end();
        log.info("consumer close run status: {}", isRunning());
//        while (true) {
//            Record message = super.take();  // 从前一个节点获取消息
//            if (message instanceof TerminateRecord) {
//                break;
//            }
//            consumerFunction.accept(message);  // 消费消息
//        }
    }
}
