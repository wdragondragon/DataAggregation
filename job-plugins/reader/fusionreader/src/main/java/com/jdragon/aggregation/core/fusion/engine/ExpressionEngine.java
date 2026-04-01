package com.jdragon.aggregation.core.fusion.engine;

import com.jdragon.aggregation.commons.element.Column;

import java.util.Map;

/**
 * 表达式引擎接口
 * 定义表达式计算的核心功能
 */
public interface ExpressionEngine {
    
    /**
     * 初始化表达式引擎
     */
    void initialize();
    
    /**
     * 编译表达式
     * @param expression 表达式字符串
     * @param contextType 上下文类型（用于类型检查）
     * @return 编译后的表达式ID或对象
     */
    Object compile(String expression, Class<?> contextType);
    
    /**
     * 执行表达式
     * @param compiledExpr 编译后的表达式
     * @param context 执行上下文
     * @return 计算结果
     */
    Object evaluate(Object compiledExpr, Object context);
    
    /**
     * 编译并执行表达式（简化方法）
     * @param expression 表达式字符串
     * @param context 执行上下文
     * @return 计算结果
     */
    Object evaluate(String expression, Object context);
    
    /**
     * 编译并执行表达式，返回Column类型结果
     * @param expression 表达式字符串
     * @param context 执行上下文
     * @param resultType 期望的结果类型
     * @return Column类型的计算结果
     */
    Column evaluateToColumn(String expression, Object context, Column.Type resultType);
    
    /**
     * 验证表达式语法
     * @param expression 表达式字符串
     * @return 验证结果，null表示验证通过，否则返回错误信息
     */
    String validate(String expression);
    
    /**
     * 获取支持的函数列表
     */
    String[] getSupportedFunctions();
    
    /**
     * 获取表达式引用的字段列表
     * @param expression 表达式字符串
     * @return 引用的字段列表（格式：sourceId.fieldName）
     */
    String[] getReferencedFields(String expression);
    
    /**
     * 注册自定义函数
     * @param functionName 函数名
     * @param functionClass 函数实现类
     */
    void registerFunction(String functionName, Class<?> functionClass);
    
    /**
     * 设置表达式缓存大小
     * @param cacheSize 缓存大小
     */
    void setCacheSize(int cacheSize);
    
    /**
     * 清理表达式缓存
     */
    void clearCache();
    
    /**
     * 获取引擎名称
     */
    String getEngineName();
    
    /**
     * 获取引擎版本
     */
    String getEngineVersion();
}