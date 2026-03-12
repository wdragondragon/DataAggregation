package com.jdragon.aggregation.core.fusion;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.consistency.model.DataSourceConfig;
import com.jdragon.aggregation.core.consistency.service.DataFetcher;
import com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import com.jdragon.aggregation.core.fusion.config.SourceConfig;
import com.jdragon.aggregation.core.fusion.strategy.FusionStrategyFactory;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 数据融合读取器
 * 支持从多个数据源读取数据并进行水平融合（JOIN）
 */
public class FusionReader extends Reader.Job {

    private FusionConfig fusionConfig;
    private DataFetcher dataFetcher;
    private FusionContext fusionContext;

    @Override
    public void init() {
        Configuration pluginJobConf = this.getPluginJobConf();
        fusionConfig = FusionConfig.fromConfig(pluginJobConf);

        // 验证融合配置
        fusionConfig.validate();

        // 初始化数据源插件管理器
        DataSourcePluginManager pluginManager = new DataSourcePluginManager();
        dataFetcher = new DataFetcher(pluginManager, true);

        // 初始化融合上下文
        fusionContext = new FusionContext(fusionConfig);
        List<String> targetColumns = getPeerPluginJobConf().getList("columns", String.class);
        fusionContext.setTargetColumns(targetColumns);

        // 初始化融合策略
        FusionStrategyFactory.initDefaultStrategies();


    }

    @Override
    public void prepare() {
        // 可以在这里进行数据源连接测试
        // 暂时不实现
    }

    @Override
    public void startRead(RecordSender recordSender) {
        try {
            // 1. 从各数据源获取数据
            Map<String, List<Map<String, Object>>> sourceData = fetchDataFromSources();


            // 2. 执行数据融合
            List<Record> fusedRecords = performFusion(sourceData);

            // 3. 发送融合后的记录
            for (Record record : fusedRecords) {
                recordSender.sendToWriter(record);
            }

        } catch (Exception e) {
            throw new RuntimeException("数据融合失败", e);
        }
    }

    /**
     * 从各数据源获取数据
     */
    private Map<String, List<Map<String, Object>>> fetchDataFromSources() {
        List<DataSourceConfig> dataSourceConfigs = convertToDataSourceConfigs();
        return dataFetcher.fetchDataFromSources(dataSourceConfigs);
    }

    /**
     * 将SourceConfig列表转换为DataSourceConfig列表
     */
    private List<DataSourceConfig> convertToDataSourceConfigs() {
        List<DataSourceConfig> configs = new ArrayList<>();
        for (SourceConfig source : fusionConfig.getSources()) {
            DataSourceConfig dsConfig = new DataSourceConfig();
            dsConfig.setSourceId(source.getSourceId());
            dsConfig.setPluginName(source.getPluginType());
            dsConfig.setConnectionConfig(source.getPluginConfig());
            dsConfig.setQuerySql(source.getQuerySql());
            dsConfig.setTableName(source.getTableName());
            // 设置其他字段...
            configs.add(dsConfig);
        }
        return configs;
    }

    /**
     * 执行数据融合
     */
    private List<Record> performFusion( Map<String, List<Map<String, Object>>> sourceData) {
        // 使用DataFetcher进行分组
        DataFetcher dataFetcher = new DataFetcher(new com.jdragon.aggregation.core.consistency.service.DataSourcePluginManager());
        Map<String, Map<String, List<Map<String, Object>>>> groupedData =
                dataFetcher.groupByMatchKeys(sourceData, fusionConfig.getJoinKeys());

        // 使用融合引擎进行融合
        FusionEngine fusionEngine = new FusionEngine(fusionConfig, fusionContext);
        return fusionEngine.fuse(groupedData);
    }
}