package com.jdragon.aggregation.core.test;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.job.JobContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;


public class InfluxTest {

    private final static Logger log = LoggerFactory.getLogger(InfluxTest.class);

    public static void main(String[] args) {
        Configuration configuration = Configuration.from(new File("C:\\dev\\ideaProject\\DataAggregation\\core\\src\\main\\resources\\toinflux.json"));
        JobContainer container = new JobContainer(configuration);
        container.start();
    }
}
