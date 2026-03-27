package com.jdragon.aggregation.core.fusion.detail;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.fusion.config.FusionConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * 融合详情记录器测试
 */
public class FusionDetailRecorderTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testHtmlGeneration() throws Exception {
        // 创建临时目录作为保存路径
        File saveDir = tempFolder.newFolder("fusion_details");
        
        // 加载demo配置（从类路径）
        java.io.InputStream is = getClass().getResourceAsStream("/fusion-mysql-demo.json");
        assertNotNull("测试资源fusion-mysql-demo.json未找到", is);
        String demoJson = new java.util.Scanner(is, "UTF-8").useDelimiter("\\A").next();
        is.close();
        
        JSONObject jsonObj = JSON.parseObject(demoJson);
        JSONObject readerConfig = jsonObj.getJSONObject("reader").getJSONObject("config");
        // 修改保存路径
        JSONObject detailConfig = readerConfig.getJSONObject("detailConfig");
        detailConfig.put("savePath", saveDir.getAbsolutePath());
        
        // 转换为Configuration
        Configuration config = Configuration.from(jsonObj);
        // 提取fusion配置部分
        Configuration fusionConfigSection = config.getConfiguration("reader.config");
        FusionConfig fusionConfig = FusionConfig.fromConfig(fusionConfigSection);
        
        // 创建记录器
        FusionDetailRecorder recorder = new FusionDetailRecorder(fusionConfig);
        
        // 添加模拟的融合详情
        FusionDetail detail1 = new FusionDetail();
        detail1.setJoinKey("user_001");
        detail1.setStatus("SUCCESS");
        
        FieldDetail field1 = new FieldDetail();
        field1.setTargetField("username");
        field1.setSourceRef("fusion_1.username");
        field1.setMappingType("DIRECT");
        field1.setFusedValue("john_doe");
        field1.setStatus("SUCCESS");
        Map<String, Object> sourceValues1 = new HashMap<>();
        sourceValues1.put("fusion_1", "john_doe");
        field1.setSourceValues(sourceValues1);
        
        detail1.getFieldDetails().add(field1);
        
        FieldDetail field2 = new FieldDetail();
        field2.setTargetField("age");
        field2.setSourceRef("fusion_2.age");
        field2.setMappingType("DIRECT");
        field2.setFusedValue(30);
        field2.setStatus("SUCCESS");
        Map<String, Object> sourceValues2 = new HashMap<>();
        sourceValues2.put("fusion_2", 30);
        field2.setSourceValues(sourceValues2);
        
        detail1.getFieldDetails().add(field2);
        
        recorder.recordDetail(detail1);
        
        // 保存详情
        String jsonFilePath = recorder.saveToFile();
        assertNotNull(jsonFilePath);
        
        // 验证JSON文件生成
        File jsonFile = new File(jsonFilePath);
        assertTrue(jsonFile.exists());
        assertTrue(jsonFile.length() > 0);
        
        // 验证HTML文件生成（与JSON同目录同前缀）
        String htmlFilePath = jsonFilePath.replace(".json", ".html");
        File htmlFile = new File(htmlFilePath);
        assertTrue(htmlFile.exists());
        assertTrue(htmlFile.length() > 0);
        
        // 验证HTML内容包含必要的标签
        String htmlContent = new String(Files.readAllBytes(htmlFile.toPath()));
        assertTrue(htmlContent.contains("<html"));
        assertTrue(htmlContent.contains("数据融合详情可视化"));
        assertTrue(htmlContent.contains("Vue"));
        assertTrue(htmlContent.contains("loadData"));
        // 确保是通用模板，没有嵌入JSON数据
        assertFalse("HTML应该不包含嵌入式JSON数据", htmlContent.contains("window.fusionData"));
        // 确保有文件选择按钮
        assertTrue(htmlContent.contains("打开JSON文件"));
        
        // 清理
        jsonFile.delete();
        htmlFile.delete();
    }
}