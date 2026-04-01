package com.jdragon.aggregation.core.test;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.job.JobContainer;

import java.io.File;

public class ConsistencyConfigExample {
    public static void main(String[] args) {
        Configuration config = Configuration.from(new File("C:\\dev\\ideaProject\\DataAggregation\\core\\src\\main\\resources\\consistency-demo.json"));
        // 解析配置
        JobContainer jobContainer = new JobContainer(config);
        jobContainer.start();
    }
}
