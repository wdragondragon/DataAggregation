package com.jdragon.aggregation.core.fusion;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.service.DataFetcher;
import com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import com.jdragon.aggregation.core.fusion.detail.FusionDetailOutput;
import com.jdragon.aggregation.core.fusion.strategy.FusionStrategyFactory;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;

import java.util.List;

/**
 * 鏁版嵁铻嶅悎璇诲彇鍣?
 * 鏀寔浠庡涓暟鎹簮璇诲彇鏁版嵁骞惰繘琛屾按骞宠瀺鍚堬紙JOIN锛?
 */
public class FusionReader extends Reader.Job {

    private FusionConfig fusionConfig;
    private DataFetcher dataFetcher;
    private FusionContext fusionContext;

    @Override
    public void init() {
        Configuration pluginJobConf = this.getPluginJobConf();
        fusionConfig = FusionConfig.fromConfig(pluginJobConf);
        fusionConfig.validate();

        DataSourcePluginManager pluginManager = new DataSourcePluginManager();
        dataFetcher = new DataFetcher(pluginManager, true);

        fusionContext = new FusionContext(fusionConfig);
        List<String> targetColumns = getPeerPluginJobConf().getList("columns", String.class);
        fusionContext.setTargetColumns(targetColumns);
        fusionContext.setJobPointReporter(this.getJobPointReporter());
        fusionContext.updateIncrValue();

        FusionStrategyFactory.initDefaultStrategies();
    }

    @Override
    public void prepare() {
        // no-op
    }

    @Override
    public void startRead(RecordSender recordSender) {
        try {
            StreamingFusionExecutor executor = new StreamingFusionExecutor(dataFetcher, fusionConfig, fusionContext);
            executor.execute(recordSender);
            fusionContext.saveFusionDetails();
        } catch (Exception e) {
            try {
                fusionContext.saveFusionDetails();
            } catch (Exception ignored) {
            }
            throw new RuntimeException("鏁版嵁铻嶅悎澶辫触", e);
        }
    }

    @Override
    public void post() {
        FusionDetailOutput.Summary summary = fusionContext.getDetailRecorder().getOutput().getSummary();
        getJobPointReporter().getTrackCommunication().addMessage("fusion_summary", JSONObject.toJSONString(summary));
    }
}
