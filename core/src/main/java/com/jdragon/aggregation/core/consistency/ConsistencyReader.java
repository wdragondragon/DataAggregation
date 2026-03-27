package com.jdragon.aggregation.core.consistency;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.OutputConfig;
import com.jdragon.aggregation.core.consistency.service.DataFetcher;
import com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager;
import com.jdragon.aggregation.core.consistency.service.FileResultRecorder;
import com.jdragon.aggregation.core.consistency.service.StreamingConsistencyExecutor;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;

import java.util.List;

public class ConsistencyReader extends Reader.Job {

    private ConsistencyRule rule;
    private StreamingConsistencyExecutor executor;
    private DataFetcher dataFetcher;
    private List<String> targetColumns;
    private final ConsistencyRecordProjector projector = new ConsistencyRecordProjector();

    @Override
    public void init() {
        Configuration configuration = getPluginJobConf();
        this.rule = ConsistencyRule.fromConfig(configuration);
        if (rule.getOutputConfig() == null) {
            OutputConfig outputConfig = new OutputConfig();
            outputConfig.setOutputPath("./consistency-results");
            rule.setOutputConfig(outputConfig);
        }

        DataSourcePluginManager pluginManager = new DataSourcePluginManager();
        this.dataFetcher = new DataFetcher(pluginManager, Boolean.TRUE.equals(rule.getParallelFetch()));
        String outputDirectory = rule.getOutputConfig().getOutputPath();
        if (outputDirectory == null || outputDirectory.trim().isEmpty()) {
            outputDirectory = "./consistency-results";
        }
        this.executor = new StreamingConsistencyExecutor(pluginManager, new FileResultRecorder(outputDirectory));
        this.targetColumns = getPeerPluginJobConf().getList("columns", String.class);
    }

    @Override
    public void startRead(RecordSender recordSender) {
        ComparisonResult result = executor.execute(rule, dataFetcher);
        List<DifferenceRecord> differences = result.getDifferenceRecords();
        if (differences != null) {
            for (DifferenceRecord difference : differences) {
                recordSender.sendToWriter(projector.project(rule.getRuleId(), difference, targetColumns));
            }
        }
        getJobPointReporter().getTrackCommunication().addMessage("consistency_summary", JSONObject.toJSONString(result.getSummary()));
        getJobPointReporter().getTrackCommunication().addMessage("consistency_result", JSONObject.toJSONString(result));
    }
}
