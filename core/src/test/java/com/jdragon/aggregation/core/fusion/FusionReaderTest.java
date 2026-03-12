package com.jdragon.aggregation.core.fusion;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * 数据融合读取器测试
 */
public class FusionReaderTest {
    
    @Test
    public void testFusionConfigParsing() {
        // 创建测试配置
        Configuration config = Configuration.newDefault();
        config.set("sources[0].id", "source1");
        config.set("sources[0].type", "mysql5");
        config.set("sources[0].config.host", "localhost");
        config.set("sources[0].weight", 1.0);
        config.set("sources[0].priority", 10);
        
        config.set("sources[1].id", "source2");
        config.set("sources[1].type", "mysql5");
        config.set("sources[1].config.host", "localhost");
        config.set("sources[1].weight", 1.5);
        config.set("sources[1].priority", 5);
        
        config.set("joinKeys[0]", "userId");
        config.set("joinType", "LEFT");
        
        config.set("fieldMappings[0].type", "DIRECT");
        config.set("fieldMappings[0].targetField", "userId");
        config.set("fieldMappings[0].sourceField", "userId");
        
        config.set("defaultStrategy", "PRIORITY");
        config.set("errorMode", "LENIENT");
        
        // 解析配置
        FusionConfig fusionConfig = FusionConfig.fromConfig(config);
        
        // 验证配置
        assertNotNull(fusionConfig);
        assertEquals(2, fusionConfig.getSources().size());
        assertEquals("source1", fusionConfig.getSources().get(0).getSourceId());
        assertEquals("source2", fusionConfig.getSources().get(1).getSourceId());
        assertEquals(FusionConfig.JoinType.LEFT, fusionConfig.getJoinType());
        assertEquals("PRIORITY", fusionConfig.getDefaultStrategy());
        assertEquals(FusionConfig.ErrorHandlingMode.LENIENT, fusionConfig.getErrorMode());
    }
    
    @Test
    public void testFusionContextCreation() {
        // 创建简单配置
        Configuration config = Configuration.newDefault();
        config.set("sources[0].id", "testSource");
        config.set("sources[0].type", "test");
        config.set("sources[0].config.host", "localhost");
        config.set("joinKeys[0]", "id");
        config.set("joinType", "INNER");
        config.set("fieldMappings[0].type", "DIRECT");
        config.set("fieldMappings[0].targetField", "id");
        config.set("fieldMappings[0].sourceField", "id");
        
        FusionConfig fusionConfig = FusionConfig.fromConfig(config);
        FusionContext context = new FusionContext(fusionConfig);
        
        assertNotNull(context);
        assertNotNull(context.getSourceConfig("testSource"));
        assertEquals(1.0, context.getSourceWeight("testSource"), 0.001);
        assertEquals(0, context.getSourcePriority("testSource"));
    }
}