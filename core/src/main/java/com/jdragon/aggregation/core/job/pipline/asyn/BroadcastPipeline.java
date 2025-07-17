package com.jdragon.aggregation.core.job.pipline.asyn;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.core.transport.channel.memory.MemoryChannel;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.core.transport.record.TerminateRecord;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class BroadcastPipeline extends PipelineAbstract {

    private final AtomicBoolean terminated = new AtomicBoolean(false);

    public BroadcastPipeline(String name, StreamHandler... consumers) {
        super(name, consumers);
        for (StreamHandler consumer : getNodes()) {
            if (consumer.getInputQueue() == null) {
                consumer.setInputQueue(new MemoryChannel());
            }
        }
    }

    @Override
    public void process() throws InterruptedException {
        StreamHandler[] consumers = getNodes();

        // 启动所有消费者线程
        for (StreamHandler consumer : consumers) {
            getExecutorService().submit(() -> {
                try {
                    consumer.process();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        // 广播线程，从本Pipeline的输入队列拉数据，然后广播到所有消费者的输入队列
        getExecutorService().submit(() -> {
            while (true) {
                Record record = getInputQueue().pull();
                if (record == null || record instanceof TerminateRecord) {
                    log.info("[BroadcastPipeline] detected termination signal, broadcasting EOF");
                    for (StreamHandler consumer : consumers) {
                        consumer.getInputQueue().push(record);
                    }
                    terminated.set(true);
                    break;
                }
                for (StreamHandler consumer : consumers) {
                    consumer.getInputQueue().push(deepCopyRecord(record));
                }
            }
        });
    }

    @Override
    public boolean isRunning() {
        if (!super.isRunning()) {
            return false;
        }
        return !terminated.get();
    }

    /**
     * 你的 Record 复制逻辑。示例实现，需根据实际Column深度复制。
     */
    private Record deepCopyRecord(Record original) {
        DefaultRecord record = (DefaultRecord) original;
        try {
            return record.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }

    }
}
