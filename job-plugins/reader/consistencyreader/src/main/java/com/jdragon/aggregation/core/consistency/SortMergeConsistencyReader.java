package com.jdragon.aggregation.core.consistency;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.ComparisonResult;
import com.jdragon.aggregation.core.consistency.model.ConsistencyRule;
import com.jdragon.aggregation.core.consistency.model.DifferenceRecord;
import com.jdragon.aggregation.core.consistency.model.OutputConfig;
import com.jdragon.aggregation.core.consistency.service.AdaptiveSortMergeConsistencyExecutor;
import com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager;
import com.jdragon.aggregation.core.consistency.service.FileResultRecorder;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;

import java.util.List;

/**
 * 实验模式 {@code consistency-sortmerge} 的 reader 入口。
 *
 * <p>负责使用现有规则配置启动自适应 sort-merge consistency 执行器，并把差异结果
 * 投影到 writer 侧。
 */
public class SortMergeConsistencyReader extends Reader.Job {

    private ConsistencyRule rule;
    private AdaptiveSortMergeConsistencyExecutor executor;
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
        String outputDirectory = rule.getOutputConfig().getOutputPath();
        if (outputDirectory == null || outputDirectory.trim().isEmpty()) {
            outputDirectory = "./consistency-results";
        }
        this.executor = new AdaptiveSortMergeConsistencyExecutor(pluginManager, new FileResultRecorder(outputDirectory));
        this.targetColumns = getPeerPluginJobConf().getList("columns", String.class);
    }

    @Override
    public void startRead(RecordSender recordSender) {
        ComparisonResult result = executor.execute(rule);
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
