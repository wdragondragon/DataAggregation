package com.jdragon.aggregation.core.fusion.config;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.core.fusion.FusionContext;
import com.jdragon.aggregation.core.fusion.engine.JaninoExpressionEngine;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Arrays;
import java.util.Map;

/**
 * 常量字段映射
 * 目标字段为固定值
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class ConstantFieldMapping extends FieldMapping {
    
    private String constantValue; // 常量值
    
    /**
     * 从Configuration解析ConstantFieldMapping
     */
    public static ConstantFieldMapping fromConfig(Configuration config) {
        ConstantFieldMapping mapping = new ConstantFieldMapping();
        mapping.setMappingType(MappingType.CONSTANT);
        mapping.setTargetField(config.getString("targetField"));
        mapping.setConstantValue(config.getString("value"));
        mapping.setResultType(config.getString("resultType", "STRING"));
        mapping.setErrorMode(config.getString("errorMode", "DEFAULT"));
        mapping.setFusionStrategy(config.getString("strategy"));
        return mapping;
    }
    
    @Override
    public Column map(Map<String, Map<String, Column>> sourceValues, FusionContext context) {
        try {
            // 确定结果列类型
            Column.Type columnType = getResultColumnType();
            
            // 使用表达式引擎的类型转换功能将常量值转换为指定类型的Column
            JaninoExpressionEngine engine = JaninoExpressionEngine.getDefaultEngine();
            Column result = engine.convertToColumn(constantValue, columnType);
            
            if (result == null) {
                return context.handleFieldError(targetField,
                    "常量值转换失败: " + constantValue + " -> " + columnType, null);
            }
            
            return result;
            
        } catch (Exception e) {
            return context.handleFieldError(targetField, 
                "常量映射执行失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 获取结果列类型
     */
    private Column.Type getResultColumnType() {
        if (resultType == null || resultType.trim().isEmpty()) {
            return Column.Type.STRING;
        }
        try {
            return Column.Type.valueOf(resultType.toUpperCase());
        } catch (IllegalArgumentException e) {
            // 如果枚举值不存在，默认使用STRING
            return Column.Type.STRING;
        }
    }
    
    @Override
    public void validate() {
        if (targetField == null || targetField.trim().isEmpty()) {
            throw new IllegalArgumentException("目标字段不能为空");
        }
        
        if (constantValue == null) {
            throw new IllegalArgumentException("常量值不能为null");
        }
        
        // 验证结果类型（如果指定）
        if (resultType != null && !resultType.trim().isEmpty()) {
            try {
                Column.Type columnType = Column.Type.valueOf(resultType.toUpperCase());
                // 对某些类型进行基本格式验证
                validateConstantValue(constantValue, columnType);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("无效的结果类型: " + resultType + "，支持的类型: " + 
                    Arrays.toString(Column.Type.values()));
            }
        }
    }
    
    /**
     * 验证常量值格式是否符合结果类型
     */
    private void validateConstantValue(String value, Column.Type columnType) {
        if (value == null) return;
        
        try {
            switch (columnType) {
                case INT:
                case LONG:
                    Long.parseLong(value);
                    break;
                case DOUBLE:
                    Double.parseDouble(value);
                    break;
                case BOOL:
                    // 布尔值接受 true/false, 1/0, yes/no 等，转换时会处理
                    break;
                case DATE:
                    // 日期格式验证较复杂，交给转换时处理
                    break;
                case BYTES:
                    // 字节数组通常来自Base64编码，这里不验证
                    break;
                // STRING, OBJECT, BAD, NULL 不需要验证
                default:
                    break;
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("常量值格式错误: '" + value + "' 无法转换为类型 " + columnType);
        }
    }
    
    @Override
    public String toString() {
        return String.format("ConstantMapping{target='%s', value='%s'}", targetField, constantValue);
    }
}