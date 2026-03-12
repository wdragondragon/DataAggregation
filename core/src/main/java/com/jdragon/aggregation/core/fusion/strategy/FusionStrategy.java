package com.jdragon.aggregation.core.fusion.strategy;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.core.fusion.FusionContext;

import java.util.Map;

/**
 * 融合策略接口
 * 定义如何从多个数据源中选择或计算字段值
 */
public interface FusionStrategy {
    
    /**
     * 获取策略名称
     */
    String getName();
    
    /**
     * 选择或计算字段值
     * @param fieldName 字段名
     * @param sourceValues 源数据值映射（sourceId -> 字段值）
     * @param context 融合上下文
     * @return 融合后的字段值
     */
    Column select(String fieldName, Map<String, Column> sourceValues, FusionContext context);
    
    /**
     * 判断策略是否适用于当前字段和源数据
     */
    default boolean canSelect(String fieldName, Map<String, Column> sourceValues, FusionContext context) {
        return true;
    }
    
    /**
     * 获取策略描述
     */
    default String getDescription() {
        return getName();
    }
}