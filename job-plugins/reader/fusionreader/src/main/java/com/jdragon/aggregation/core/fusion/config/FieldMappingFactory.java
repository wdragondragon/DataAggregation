package com.jdragon.aggregation.core.fusion.config;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.element.Column;

import java.util.HashMap;
import java.util.Map;

/**
 * 字段映射工厂
 * 统一管理FieldMapping的创建和注册
 */
public class FieldMappingFactory {
    
    private static final Map<String, Class<? extends FieldMapping>> mappingRegistry = new HashMap<>();
    
    static {
        // 注册内置的字段映射类型
        registerMappingType("DIRECT", DirectFieldMapping.class);
        registerMappingType("EXPRESSION", ExpressionFieldMapping.class);
        registerMappingType("GROOVY", GroovyFieldMapping.class);
        registerMappingType("CONDITIONAL", ConditionalFieldMapping.class);
        registerMappingType("CONSTANT", ConstantFieldMapping.class);
    }
    
    /**
     * 注册自定义字段映射类型
     */
    public static void registerMappingType(String type, Class<? extends FieldMapping> mappingClass) {
        mappingRegistry.put(type.toUpperCase(), mappingClass);
    }
    
    /**
     * 创建字段映射实例
     */
    public static FieldMapping createMapping(Configuration config) {
        String type = config.getString("type", "DIRECT").toUpperCase();
        
        Class<? extends FieldMapping> mappingClass = mappingRegistry.get(type);
        if (mappingClass == null) {
            throw new IllegalArgumentException("不支持的字段映射类型: " + type);
        }
        
        try {
            // 调用对应类的fromConfig方法
            if (DirectFieldMapping.class.equals(mappingClass)) {
                return DirectFieldMapping.fromConfig(config);
            } else if (ExpressionFieldMapping.class.equals(mappingClass)) {
                return ExpressionFieldMapping.fromConfig(config);
            } else if (GroovyFieldMapping.class.equals(mappingClass)) {
                return GroovyFieldMapping.fromConfig(config);
            } else if (ConditionalFieldMapping.class.equals(mappingClass)) {
                return ConditionalFieldMapping.fromConfig(config);
            } else if (ConstantFieldMapping.class.equals(mappingClass)) {
                return ConstantFieldMapping.fromConfig(config);
            } else {
                // 对于自定义类型，尝试调用fromConfig静态方法
                try {
                    return (FieldMapping) mappingClass.getMethod("fromConfig", Configuration.class)
                            .invoke(null, config);
                } catch (Exception e) {
                    throw new IllegalArgumentException("自定义字段映射类型必须实现fromConfig静态方法: " + type, e);
                }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("创建字段映射失败: " + e.getMessage(), e);
        }
    }
    
    /**
     * 批量创建字段映射
     */
    public static FieldMapping[] createMappings(Configuration[] configs) {
        if (configs == null) {
            return new FieldMapping[0];
        }
        
        FieldMapping[] mappings = new FieldMapping[configs.length];
        for (int i = 0; i < configs.length; i++) {
            mappings[i] = createMapping(configs[i]);
        }
        return mappings;
    }
    
    /**
     * 验证字段映射配置
     */
    public static void validateMappings(FieldMapping[] mappings) {
        if (mappings == null) {
            return;
        }
        
        Map<String, Boolean> targetFields = new HashMap<>();
        for (FieldMapping mapping : mappings) {
            mapping.validate();
            
            // 检查目标字段唯一性
            String targetField = mapping.getTargetField();
            if (targetFields.containsKey(targetField)) {
                throw new IllegalArgumentException("目标字段重复: " + targetField);
            }
            targetFields.put(targetField, true);
        }
    }
    
    /**
     * 获取支持的映射类型列表
     */
    public static String[] getSupportedMappingTypes() {
        return mappingRegistry.keySet().toArray(new String[0]);
    }
}