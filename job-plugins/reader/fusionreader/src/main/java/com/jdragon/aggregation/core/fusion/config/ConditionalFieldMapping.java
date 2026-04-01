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
 * 条件字段映射
 * 根据条件选择不同的源字段或值
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class ConditionalFieldMapping extends FieldMapping {

    private String condition;       // 条件表达式
    private String trueValue;       // 条件为真时的值或字段引用
    private String falseValue;      // 条件为假时的值或字段引用

    /**
     * 从Configuration解析ConditionalFieldMapping
     */
    public static ConditionalFieldMapping fromConfig(Configuration config) {
        ConditionalFieldMapping mapping = new ConditionalFieldMapping();
        mapping.setMappingType(MappingType.CONDITIONAL);
        mapping.setTargetField(config.getString("targetField"));
        mapping.setCondition(config.getString("condition"));
        mapping.setTrueValue(config.getString("trueValue"));
        mapping.setFalseValue(config.getString("falseValue"));
        mapping.setResultType(config.getString("resultType"));
        mapping.setErrorMode(config.getString("errorMode", "DEFAULT"));
        mapping.setFusionStrategy(config.getString("strategy"));
        return mapping;
    }

    @Override
    public Column map(Map<String, Map<String, Column>> sourceValues, FusionContext context) {
        try {
            // 构建条件表达式
            String expression = buildConditionalExpression();

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
                        "条件表达式计算结果为空: " + expression, null);
            }

            return result;

        } catch (Exception e) {
            return context.handleFieldError(targetField,
                    "条件映射执行失败: " + e.getMessage(), e);
        }
    }

    /**
     * 构建条件表达式字符串
     */
    private String buildConditionalExpression() {
        // 使用IF函数构建表达式：IF(condition, trueValue, falseValue)
        return "ifExpr(" + condition + ", " + trueValue + ", " + falseValue + ")";
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

        if (condition == null || condition.trim().isEmpty()) {
            throw new IllegalArgumentException("条件表达式不能为空");
        }

        if (trueValue == null || trueValue.trim().isEmpty()) {
            throw new IllegalArgumentException("真值表达式不能为空");
        }

        if (falseValue == null || falseValue.trim().isEmpty()) {
            throw new IllegalArgumentException("假值表达式不能为空");
        }

        // 使用表达式引擎验证整个条件表达式
        JaninoExpressionEngine engine = JaninoExpressionEngine.getDefaultEngine();
        String fullExpression = buildConditionalExpression();
        String validationError = engine.validate(fullExpression);
        if (validationError != null) {
            throw new IllegalArgumentException("条件表达式语法错误: " + validationError + " (表达式: " + fullExpression + ")");
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
        return String.format("ConditionalMapping{target='%s', condition='%s', true='%s', false='%s'}",
                targetField, condition, trueValue, falseValue);
    }
}