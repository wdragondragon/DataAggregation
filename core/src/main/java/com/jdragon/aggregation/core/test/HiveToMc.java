package com.jdragon.aggregation.core.test;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.job.JobContainer;

import java.util.Arrays;

public class HiveToMc {
    public static void main(String[] args) {
        Configuration configuration = Configuration.newDefault();
        configuration.set("reader.type", "tbds-hive2");
        configuration.set("reader.config.connect.host", "");
        configuration.set("reader.config.connect.port", "");
        configuration.set("reader.config.connect.userName", "");
        configuration.set("reader.config.connect.password", "");
        configuration.set("reader.config.connect.database", "");
        configuration.set("reader.config.connect.usePool", false);
        configuration.set("reader.config.table", "");
        configuration.set("reader.config.columns", Arrays.asList("id", "name"));



        JobContainer jobContainer = new JobContainer(configuration);
        jobContainer.start();
    }
}
