package com.jdragon.aggregation.core.fusion.config;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.core.fusion.FusionContext;
import com.jdragon.aggregation.core.fusion.engine.JaninoExpressionEngine;
import lombok.Data;
import lombok.EqualsAndHashCode;

import groovy.lang.GroovyShell;
import groovy.lang.Binding;
import groovy.lang.Script;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Groovy脚本字段映射
 * 通过Groovy脚本计算目标字段值
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class GroovyFieldMapping extends FieldMapping {
    
    private String script; // Groovy脚本代码
    
    private static final Map<String, Script> SCRIPT_CACHE = new ConcurrentHashMap<>();
    private static final GroovyShell GROOVY_SHELL = new GroovyShell();
    
    /**
     * 从Configuration解析GroovyFieldMapping
     */
    public static GroovyFieldMapping fromConfig(Configuration config) {
        GroovyFieldMapping mapping = new GroovyFieldMapping();
        mapping.setMappingType(FieldMapping.MappingType.GROOVY);
        mapping.setTargetField(config.getString("targetField"));
        mapping.setScript(config.getString("script"));
        mapping.setResultType(config.getString("resultType"));
        mapping.setErrorMode(config.getString("errorMode", "DEFAULT"));
        mapping.setFusionStrategy(config.getString("strategy"));
        return mapping;
    }
    
    @Override
    public Column map(Map<String, Map<String, Column>> sourceValues, FusionContext context) {
        try {
            // 获取或编译Groovy脚本
            Script groovyScript = getCompiledScript();
            
            // 准备绑定变量：将源数据转换为易于访问的格式
            Binding binding = prepareBinding(sourceValues);
            groovyScript.setBinding(binding);
            
            // 执行脚本
            Object result = groovyScript.run();
            
            // 确定结果类型
            Column.Type columnType = getResultColumnType();
            
            // 将结果转换为Column
            JaninoExpressionEngine engine = JaninoExpressionEngine.getDefaultEngine();
            Column columnResult = engine.convertToColumn(result, columnType);
            
            if (columnResult == null) {
                return context.handleFieldError(targetField,
                    "Groovy脚本返回结果转换失败: " + result + " -> " + columnType, null);
            }
            
            return columnResult;
            
        } catch (Exception e) {
            return context.handleFieldError(targetField, 
                "Groovy脚本映射执行失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 获取编译后的Groovy脚本（缓存）
     */
    private Script getCompiledScript() {
        return SCRIPT_CACHE.computeIfAbsent(script, s -> GROOVY_SHELL.parse(s));
    }
    
    /**
     * 准备Groovy脚本的绑定变量
     */
    private Binding prepareBinding(Map<String, Map<String, Column>> sourceValues) {
        Binding binding = new Binding();
        
        // 1. 添加扁平化的字段映射：sourceId_fieldName -> value
        Map<String, Object> flatFields = new HashMap<>();
        for (Map.Entry<String, Map<String, Column>> sourceEntry : sourceValues.entrySet()) {
            String sourceId = sourceEntry.getKey();
            Map<String, Column> fields = sourceEntry.getValue();
            if (fields != null) {
                for (Map.Entry<String, Column> fieldEntry : fields.entrySet()) {
                    String key = sourceId + "_" + fieldEntry.getKey();
                    flatFields.put(key, convertColumnToObject(fieldEntry.getValue()));
                }
            }
        }
        binding.setVariable("fields", flatFields);
        
        // 2. 添加按源分组的字段映射：sourceId -> Map<fieldName, value>
        Map<String, Map<String, Object>> groupedFields = new HashMap<>();
        for (Map.Entry<String, Map<String, Column>> sourceEntry : sourceValues.entrySet()) {
            String sourceId = sourceEntry.getKey();
            Map<String, Column> fields = sourceEntry.getValue();
            if (fields != null) {
                Map<String, Object> fieldMap = new HashMap<>();
                for (Map.Entry<String, Column> fieldEntry : fields.entrySet()) {
                    fieldMap.put(fieldEntry.getKey(), convertColumnToObject(fieldEntry.getValue()));
                }
                groupedFields.put(sourceId, fieldMap);
            }
        }
        binding.setVariable("sources", groupedFields);
        
        // 3. 添加当前目标字段名（方便脚本使用）
        binding.setVariable("targetField", targetField);
        
        return binding;
    }
    
    /**
     * 将Column转换为Java对象
     */
    private Object convertColumnToObject(Column column) {
        if (column == null) {
            return null;
        }
        switch (column.getType()) {
            case INT:
            case LONG:
            case DOUBLE:
                return column.asDouble();
            case STRING:
                return column.asString();
            case BOOL:
                return column.asBoolean();
            case DATE:
                return column.asDate();
            case BYTES:
                return column.asBytes();
            default:
                return column.asString();
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
        
        if (script == null || script.trim().isEmpty()) {
            throw new IllegalArgumentException("Groovy脚本不能为空");
        }
        
        // 尝试编译Groovy脚本以验证语法
        try {
            GROOVY_SHELL.parse(script);
        } catch (Exception e) {
            throw new IllegalArgumentException("Groovy脚本语法错误: " + e.getMessage() + " (脚本: " + 
                (script.length() > 100 ? script.substring(0, 97) + "..." : script) + ")", e);
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
        String scriptPreview = script.length() > 50 ? script.substring(0, 47) + "..." : script;
        return String.format("GroovyMapping{target='%s', script='%s'}", targetField, scriptPreview);
    }
}