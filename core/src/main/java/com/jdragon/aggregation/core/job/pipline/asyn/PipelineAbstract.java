package com.jdragon.aggregation.core.job.pipline.asyn;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@EqualsAndHashCode(callSuper = true)
@Data
public class PipelineAbstract extends StreamHandler {
    private String pipelineName;
    private final ExecutorService executorService;
    private final StreamHandler[] nodes;  // DAG图的节点

    public PipelineAbstract(String pipelineName, StreamHandler[] nodes) {
        this.pipelineName = pipelineName;
        BasicThreadFactory tf = new BasicThreadFactory.Builder().namingPattern(pipelineName + "-%d").build();
        this.executorService = Executors.newCachedThreadPool(tf);
        this.nodes = nodes;
    }

    public void start() throws InterruptedException {
        process();
        while (isRunning()) {
            Thread.sleep(500);
            log.info("pipeline [{}] running...", pipelineName);
        }
        log.info("pipeline [{}] finished.", pipelineName);
        stop();
    }

    @Override
    public void process() throws InterruptedException {

    }

    public void stop() {
        for (StreamHandler handler : this.nodes) {
            handler.stop();
        }
        executorService.shutdownNow();
    }

    @Override
    public boolean isRunning() {
        return Arrays.stream(getNodes()).anyMatch(StreamHandler::isRunning);
    }
}
