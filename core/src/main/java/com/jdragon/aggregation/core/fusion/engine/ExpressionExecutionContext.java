package com.jdragon.aggregation.core.fusion.engine;

import com.jdragon.aggregation.commons.element.Column;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * 表达式执行上下文
 * 提供表达式执行时对多源字段的访问
 */
@Data
public class ExpressionExecutionContext {
    
    private Map<String, Map<String, Column>> sourceFields; // 源字段数据
    private Map<String, Object> variables;                 // 自定义变量
    private Map<String, Object> parameters;               // 执行参数
    
    public ExpressionExecutionContext() {
        this.sourceFields = new HashMap<>();
        this.variables = new HashMap<>();
        this.parameters = new HashMap<>();
    }
    
    public ExpressionExecutionContext(Map<String, Map<String, Column>> sourceFields) {
        this();
        this.sourceFields = sourceFields != null ? sourceFields : new HashMap<>();
    }
    
    /**
     * 获取字段值
     * @param fieldRef 字段引用，格式：sourceId.fieldName 或 variableName
     * @return 字段值
     */
    public Object getValue(String fieldRef) {
        // 检查是否是源字段引用
        if (fieldRef.contains(".")) {
            String[] parts = fieldRef.split("\\.");
            if (parts.length == 2) {
                String sourceId = parts[0];
                String fieldName = parts[1];
                
                Map<String, Column> sourceData = sourceFields.get(sourceId);
                if (sourceData != null) {
                    Column column = sourceData.get(fieldName);
                    if (column != null) {
                        return convertColumnToObject(column);
                    }
                }
            }
        }
        
        // 检查是否是变量
        if (variables.containsKey(fieldRef)) {
            return variables.get(fieldRef);
        }
        
        // 检查是否是参数
        if (parameters.containsKey(fieldRef)) {
            return parameters.get(fieldRef);
        }
        
        return null;
    }
    
    /**
     * 获取数值型字段值
     */
    public Number getNumber(String fieldRef) {
        Object value = getValue(fieldRef);
        if (value == null) {
            return null;
        }
        
        if (value instanceof Number) {
            return (Number) value;
        }
        
        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        
        return null;
    }
    
    /**
     * 获取字符串型字段值
     */
    public String getString(String fieldRef) {
        Object value = getValue(fieldRef);
        if (value == null) {
            return null;
        }
        
        return value.toString();
    }
    
    /**
     * 获取布尔型字段值
     */
    public Boolean getBoolean(String fieldRef) {
        Object value = getValue(fieldRef);
        if (value == null) {
            return null;
        }
        
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        
        if (value instanceof String) {
            String str = ((String) value).toLowerCase();
            return "true".equals(str) || "1".equals(str) || "yes".equals(str);
        }
        
        if (value instanceof Number) {
            return ((Number) value).doubleValue() != 0.0;
        }
        
        return null;
    }
    
    /**
     * 设置变量值
     */
    public void setVariable(String name, Object value) {
        variables.put(name, value);
    }
    
    /**
     * 设置参数值
     */
    public void setParameter(String name, Object value) {
        parameters.put(name, value);
    }
    
    /**
     * 添加源字段数据
     */
    public void addSourceFields(String sourceId, Map<String, Column> fields) {
        sourceFields.put(sourceId, fields);
    }
    
    /**
     * 获取所有源ID
     */
    public String[] getSourceIds() {
        return sourceFields.keySet().toArray(new String[0]);
    }
    
    /**
     * 获取源的所有字段名
     */
    public String[] getFieldNames(String sourceId) {
        Map<String, Column> fields = sourceFields.get(sourceId);
        if (fields == null) {
            return new String[0];
        }
        return fields.keySet().toArray(new String[0]);
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
     * 创建字段引用
     */
    public static String createFieldRef(String sourceId, String fieldName) {
        return sourceId + "." + fieldName;
    }
    
    /**
     * 解析字段引用
     */
    public static String[] parseFieldRef(String fieldRef) {
        if (fieldRef == null || !fieldRef.contains(".")) {
            return new String[] { fieldRef, null };
        }
        
        String[] parts = fieldRef.split("\\.", 2);
        return new String[] { parts[0], parts[1] };
    }
}