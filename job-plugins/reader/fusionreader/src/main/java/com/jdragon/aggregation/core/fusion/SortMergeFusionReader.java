package com.jdragon.aggregation.core.fusion;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import com.jdragon.aggregation.core.fusion.detail.FusionDetailOutput;
import com.jdragon.aggregation.core.fusion.strategy.FusionStrategyFactory;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.sortmerge.SortMergeStats;

import java.util.List;

/**
 * 实验模式 {@code fusion-sortmerge} 的 reader 入口。
 *
 * <p>负责把现有 fusion 配置转换为自适应 sort-merge 执行参数，并把执行统计回写到
 * job 上下文。
 */
public class SortMergeFusionReader extends Reader.Job {

    private FusionConfig fusionConfig;
    private FusionContext fusionContext;
    private DataSourcePluginManager pluginManager;

    @Override
    public void init() {
        Configuration pluginJobConf = this.getPluginJobConf();
        fusionConfig = FusionConfig.fromConfig(pluginJobConf);
        fusionConfig.validate();

        pluginManager = new DataSourcePluginManager();
        fusionContext = new FusionContext(fusionConfig);
        List<String> targetColumns = getPeerPluginJobConf().getList("columns", String.class);
        fusionContext.setTargetColumns(targetColumns);
        fusionContext.setJobPointReporter(this.getJobPointReporter());
        fusionContext.updateIncrValue();

        FusionStrategyFactory.initDefaultStrategies();
    }

    @Override
    public void startRead(RecordSender recordSender) {
        try {
            AdaptiveSortMergeFusionExecutor executor = new AdaptiveSortMergeFusionExecutor(pluginManager, fusionConfig, fusionContext);
            SortMergeStats stats = executor.execute(recordSender);
            fusionContext.saveFusionDetails();
            if (getJobPointReporter() != null && getJobPointReporter().getTrackCommunication() != null) {
                getJobPointReporter().getTrackCommunication().addMessage("fusion_sortmerge_summary", JSONObject.toJSONString(stats));
            }
        } catch (Exception e) {
            try {
                fusionContext.saveFusionDetails();
            } catch (Exception ignored) {
            }
            throw new RuntimeException("Adaptive sort-merge fusion failed", e);
        }
    }

    @Override
    public void post() {
        FusionDetailOutput.Summary summary = fusionContext.getDetailRecorder().getOutput().getSummary();
        getJobPointReporter().getTrackCommunication().addMessage("fusion_summary", JSONObject.toJSONString(summary));
    }
}
