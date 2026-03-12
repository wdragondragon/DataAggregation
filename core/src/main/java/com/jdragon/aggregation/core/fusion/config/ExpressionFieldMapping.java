package com.jdragon.aggregation.core.fusion.config;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.core.fusion.FusionContext;
import com.jdragon.aggregation.core.fusion.engine.JaninoExpressionEngine;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Arrays;
import java.util.Map;

/**
 * 表达式字段映射
 * 通过表达式计算目标字段值，如：sourceA.price * sourceB.quantity
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class ExpressionFieldMapping extends FieldMapping {
    
    private String expression; // 表达式字符串
    
    /**
     * 从Configuration解析ExpressionFieldMapping
     */
    public static ExpressionFieldMapping fromConfig(Configuration config) {
        ExpressionFieldMapping mapping = new ExpressionFieldMapping();
        mapping.setMappingType(MappingType.EXPRESSION);
        mapping.setTargetField(config.getString("targetField"));
        mapping.setExpression(config.getString("expression"));
        mapping.setResultType(config.getString("resultType"));
        mapping.setErrorMode(config.getString("errorMode", "DEFAULT"));
        mapping.setFusionStrategy(config.getString("strategy"));
        return mapping;
    }
    
    @Override
    public Column map(Map<String, Map<String, Column>> sourceValues, FusionContext context) {
        try {
            // 获取表达式引擎
            JaninoExpressionEngine engine = JaninoExpressionEngine.getDefaultEngine();
            
            // 创建表达式执行上下文
            com.jdragon.aggregation.core.fusion.engine.ExpressionExecutionContext execContext = 
                new com.jdragon.aggregation.core.fusion.engine.ExpressionExecutionContext(sourceValues);
            
            // 确定结果类型
            Column.Type columnType = getResultColumnType();
            
            // 计算表达式并转换为Column
            Column result = engine.evaluateToColumn(expression, execContext, columnType);
            
            if (result == null) {
                return context.handleFieldError(targetField,
                    "表达式计算结果为空: " + expression, null);
            }
            
            return result;
            
        } catch (Exception e) {
            return context.handleFieldError(targetField, 
                "表达式映射执行失败: " + e.getMessage(), e);
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
        
        if (expression == null || expression.trim().isEmpty()) {
            throw new IllegalArgumentException("表达式不能为空");
        }
        
        // 使用表达式引擎验证表达式语法
        JaninoExpressionEngine engine = JaninoExpressionEngine.getDefaultEngine();
        String validationError = engine.validate(expression);
        if (validationError != null) {
            throw new IllegalArgumentException("表达式语法错误: " + validationError + " (表达式: " + expression + ")");
        }
        
        // 验证结果类型（如果指定）
        if (resultType != null && !resultType.trim().isEmpty()) {
            try {
                Column.Type.valueOf(resultType.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("无效的结果类型: " + resultType + "，支持的类型: " + 
                    Arrays.toString(Column.Type.values()));
            }
        }
    }
    
    @Override
    public String toString() {
        return String.format("ExpressionMapping{target='%s', expression='%s'}", targetField, expression);
    }
}