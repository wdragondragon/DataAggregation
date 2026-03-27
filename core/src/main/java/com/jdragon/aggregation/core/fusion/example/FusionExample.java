package com.jdragon.aggregation.core.fusion.example;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.fusion.FusionReader;
import com.jdragon.aggregation.core.job.JobContainer;
import com.jdragon.aggregation.core.plugin.PluginType;

import java.io.File;

public class FusionExample {
    public static void main(String[] args) {
        Configuration config = Configuration.from(new File("C:\\dev\\ideaProject\\DataAggregation\\core\\src\\main\\resources\\fusion-mysql-demo.json"));
        // 解析配置
        JobContainer jobContainer = new JobContainer(config);
        jobContainer.start();
    }
}
