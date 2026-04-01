package com.jdragon.aggregation.core.fusion.config;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.core.fusion.FusionContext;
import lombok.Data;

import java.util.Map;

/**
 * 字段映射基类
 * 定义从源字段到目标字段的映射规则
 */
@Data
public abstract class FieldMapping {
    
    /**
     * 映射类型枚举
     */
    public enum MappingType {
        DIRECT,      // 直接映射：源字段直接复制到目标字段
        EXPRESSION,  // 表达式映射：通过表达式计算目标字段值
        GROOVY,      // Groovy脚本映射：通过Groovy脚本计算目标字段值
        CONDITIONAL, // 条件映射：根据条件选择不同的源字段
        CONSTANT     // 常量映射：目标字段为固定值
    }
    
    protected String targetField;              // 目标字段名
    protected MappingType mappingType;         // 映射类型
    protected String resultType;               // 结果数据类型
    protected String errorMode = "DEFAULT";    // 错误处理模式
    protected String fusionStrategy;           // 字段级融合策略（可选）
    
    /**
     * 执行字段映射
     * @param sourceValues 源数据值映射（sourceId -> 字段值映射）
     * @param context 融合上下文
     * @return 映射后的字段值
     */
    public abstract Column map(Map<String, Map<String, Column>> sourceValues, FusionContext context);
    
    /**
     * 从Configuration解析FieldMapping
     */
    public static FieldMapping fromConfig(Configuration config) {
        String type = config.getString("type", "DIRECT").toUpperCase();
        
        switch (type) {
            case "DIRECT":
                return DirectFieldMapping.fromConfig(config);
            case "EXPRESSION":
                return ExpressionFieldMapping.fromConfig(config);
            case "GROOVY":
                return GroovyFieldMapping.fromConfig(config);
            case "CONDITIONAL":
                return ConditionalFieldMapping.fromConfig(config);
            case "CONSTANT":
                return ConstantFieldMapping.fromConfig(config);
            default:
                throw new IllegalArgumentException("不支持的字段映射类型: " + type);
        }
    }
    
    /**
     * 验证配置有效性
     */
    public abstract void validate();
    
    /**
     * 获取映射类型的描述
     */
    public String getTypeDescription() {
        switch (mappingType) {
            case DIRECT:
                return "直接映射";
            case EXPRESSION:
                return "表达式映射";
            case GROOVY:
                return "Groovy脚本映射";
            case CONDITIONAL:
                return "条件映射";
            case CONSTANT:
                return "常量映射";
            default:
                return "未知映射类型";
        }
    }
    
    /**
     * 解析错误处理模式
     */
    protected FusionContext.ErrorHandlingMode parseErrorMode(String modeStr) {
        if (modeStr == null || "DEFAULT".equalsIgnoreCase(modeStr)) {
            return null; // 使用全局默认
        }
        
        try {
            return FusionContext.ErrorHandlingMode.valueOf(modeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("无效的错误处理模式: " + modeStr);
        }
    }
}