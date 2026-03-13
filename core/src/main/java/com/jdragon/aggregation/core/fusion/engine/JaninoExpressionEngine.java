package com.jdragon.aggregation.core.fusion.engine;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.element.LongColumn;
import com.jdragon.aggregation.commons.element.DoubleColumn;
import com.jdragon.aggregation.commons.element.BoolColumn;
import com.jdragon.aggregation.commons.element.DateColumn;
import com.jdragon.aggregation.commons.element.BytesColumn;
import com.jdragon.aggregation.commons.element.ObjectColumn;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.janino.ExpressionEvaluator;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Janino表达式引擎实现
 * 使用Janino编译器动态编译和执行Java表达式
 */
@Slf4j
public class JaninoExpressionEngine implements ExpressionEngine {

    private static final String ENGINE_NAME = "JaninoExpressionEngine";
    private static final String ENGINE_VERSION = "1.0";

    private final Map<String, CompiledExpression> expressionCache = new ConcurrentHashMap<>();
    private int cacheSize = 1000;

    private final Map<String, Class<?>> customFunctions = new HashMap<>();

    private static final JaninoExpressionEngine DEFAULT_ENGINE = new JaninoExpressionEngine();

    static {
        DEFAULT_ENGINE.initialize();
    }

    public static JaninoExpressionEngine getDefaultEngine() {
        return DEFAULT_ENGINE;
    }

    @Override
    public void initialize() {
        log.info("初始化Janino表达式引擎");
        // 预编译一些常用表达式模板
        precompileTemplates();
    }

    @Override
    public Object compile(String expression, Class<?> contextType) {
        if (expression == null || expression.trim().isEmpty()) {
            throw new IllegalArgumentException("表达式不能为空");
        }

        String cacheKey = generateCacheKey(expression, contextType);
        CompiledExpression compiled = expressionCache.get(cacheKey);

        if (compiled != null) {
            return compiled;
        }

        try {
            // 预处理表达式：替换函数调用，添加类型转换等
            String processedExpr = preprocessExpression(expression);

            // 创建表达式求值器
            ExpressionEvaluator evaluator = new ExpressionEvaluator();

            // 设置参数类型和名称
            evaluator.setParameters(new String[]{"ctx"}, new Class[]{contextType});

            // 设置表达式返回类型为Object（最通用）
            evaluator.setExpressionType(Object.class);

            // 设置默认导入，使表达式可以直接使用FunctionRegistry和ExpressionExecutionContext
            evaluator.setDefaultImports(new String[]{
                    "com.jdragon.aggregation.core.fusion.engine.FunctionRegistry",
                    "com.jdragon.aggregation.core.fusion.engine.ExpressionExecutionContext",
                    "static com.jdragon.aggregation.core.fusion.engine.FunctionRegistry.*"
            });

            // 设置父类加载器，确保可以加载相关类
            evaluator.setParentClassLoader(FunctionRegistry.class.getClassLoader());

            // 编译表达式
            evaluator.cook(processedExpr);

            compiled = new CompiledExpression(evaluator, processedExpr);

            // 缓存编译结果
            if (expressionCache.size() >= cacheSize) {
                // 简单的LRU策略：移除最早的条目
                if (!expressionCache.isEmpty()) {
                    String firstKey = expressionCache.keySet().iterator().next();
                    expressionCache.remove(firstKey);
                }
            }

            expressionCache.put(cacheKey, compiled);
            log.debug("编译表达式成功: {}", expression);

            return compiled;

        } catch (Exception e) {
            throw new RuntimeException("表达式编译失败: " + expression, e);
        }
    }

    @Override
    public Object evaluate(Object compiledExpr, Object context) {
        if (!(compiledExpr instanceof CompiledExpression)) {
            throw new IllegalArgumentException("无效的编译表达式对象");
        }

        CompiledExpression compiled = (CompiledExpression) compiledExpr;

        try {
            return compiled.evaluator.evaluate(new Object[]{context});
        } catch (Exception e) {
            throw new RuntimeException("表达式执行失败: " + compiled.originalExpression, e);
        }
    }

    @Override
    public Object evaluate(String expression, Object context) {
        Object compiled = compile(expression, context.getClass());
        return evaluate(compiled, context);
    }

    @Override
    public Column evaluateToColumn(String expression, Object context, Column.Type resultType) {
        Object result = evaluate(expression, context);
        return convertToColumn(result, resultType);
    }

    @Override
    public String validate(String expression) {
        try {
            // 尝试编译但不缓存
            String processedExpr = preprocessExpression(expression);
            ExpressionEvaluator evaluator = new ExpressionEvaluator();
            evaluator.setParameters(new String[]{"ctx"}, new Class[]{ExpressionExecutionContext.class});
            evaluator.setExpressionType(Object.class);
            evaluator.setDefaultImports(new String[]{
                    "com.jdragon.aggregation.core.fusion.engine.FunctionRegistry",
                    "com.jdragon.aggregation.core.fusion.engine.ExpressionExecutionContext",
                    "static com.jdragon.aggregation.core.fusion.engine.FunctionRegistry.*"
            });
            evaluator.setParentClassLoader(FunctionRegistry.class.getClassLoader());
            evaluator.cook(processedExpr);
            return null; // 验证通过
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @Override
    public String[] getSupportedFunctions() {
        return FunctionRegistry.getRegisteredFunctions();
    }

    @Override
    public String[] getReferencedFields(String expression) {
        // 简单实现：提取所有 ${sourceId.fieldName} 模式的引用
        // 实际应该使用更复杂的解析
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\$\\{([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_][a-zA-Z0-9_]*)\\}");
        java.util.regex.Matcher matcher = pattern.matcher(expression);
        java.util.Set<String> fields = new java.util.HashSet<>();

        while (matcher.find()) {
            fields.add(matcher.group(1));
        }

        return fields.toArray(new String[0]);
    }

    @Override
    public void registerFunction(String functionName, Class<?> functionClass) {
        customFunctions.put(functionName, functionClass);
        log.info("注册自定义函数: {} -> {}", functionName, functionClass.getName());
    }

    @Override
    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
        // 如果缓存超限，清理部分缓存
        while (expressionCache.size() > cacheSize) {
            String firstKey = expressionCache.keySet().iterator().next();
            expressionCache.remove(firstKey);
        }
    }

    @Override
    public void clearCache() {
        expressionCache.clear();
        log.info("清理表达式缓存");
    }

    @Override
    public String getEngineName() {
        return ENGINE_NAME;
    }

    @Override
    public String getEngineVersion() {
        return ENGINE_VERSION;
    }

    // ========== 私有方法 ==========

    /**
     * 预处理表达式
     */
    private String preprocessExpression(String expression) {
        String processed = expression;

        // 1. 替换函数调用为Java方法调用
        processed = replaceFunctionCalls(processed);

        // 2. 处理字段引用：sourceId.fieldName -> ctx.getValue("sourceId.fieldName")
        processed = replaceFieldReferences(processed);

        // 3. 添加必要的类型转换
        processed = addTypeConversions(processed);

        // 4. 处理特殊运算符
        processed = replaceOperators(processed);

        log.debug("表达式预处理: {} -> {}", expression, processed);
        return processed;
    }

    /**
     * 替换函数调用
     */
    private String replaceFunctionCalls(String expression) {
        // 获取所有注册的函数
        String[] functions = FunctionRegistry.getRegisteredFunctions();

        String result = expression;
        for (String func : functions) {
            // 简单的函数名替换（实际应该使用更复杂的语法分析）
            // 格式: FUNC(arg1, arg2) -> FunctionRegistry.func(arg1, arg2)
            String pattern = "\\b" + func + "\\s*\\(";
            String replacement = "FunctionRegistry." + getMethodName(func) + "(";
            result = result.replaceAll(pattern, replacement);
        }

        return result;
    }

    /**
     * 获取函数对应的方法名
     */
    private String getMethodName(String functionName) {
        // 将函数名转换为方法名（通常是首字母小写）
        if (functionName == null || functionName.isEmpty()) {
            return functionName;
        }

//        String lowerName = functionName.toLowerCase();

        // 特殊处理一些函数名
        switch (functionName) {
            case "IF":
                return "ifExpr";
            case "CASE":
                return "caseExpr";
            default:
                return functionName;
        }
    }

    /**
     * 替换字段引用
     */
    private String replaceFieldReferences(String expression) {
        // 匹配 ${sourceId.fieldName} 模式
//        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("\\$\\{([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_][a-zA-Z0-9_]*)}");
        Pattern pattern = Pattern.compile("\\$\\{([a-zA-Z_]\\w*(?:\\.\\w+)?(?:,\\s*\\w+)?)}");
        java.util.regex.Matcher matcher = pattern.matcher(expression);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            String fieldRef = matcher.group(1);
            String[] split = fieldRef.split(",");
            String method = "getValue";
            if (split.length == 2) {
                method = "get" + split[1].trim();
            }
            String replacement = "ctx." + method + "(\"" + split[0] + "\")";
            matcher.appendReplacement(sb, replacement);
        }
        matcher.appendTail(sb);

        return sb.toString();
    }

    /**
     * 添加类型转换
     */
    private String addTypeConversions(String expression) {
        String result = expression;

//        // 处理字段引用与数字字面量的运算：ctx.getValue("field") * 2
//        result = result.replaceAll(
//                "(ctx\\.getValue\\(\"[^\"]+\"\\))\\s*([+\\-*/%])\\s*([0-9]+\\.?[0-9]*)",
//                "($1 != null ? ((Number)$1).doubleValue() : 0.0) $2 $3"
//        );

//        // 处理数字字面量与字段引用的运算：2 * ctx.getValue("field")
//        result = result.replaceAll(
//                "([0-9]+\\.?[0-9]*)\\s*([+\\-*/%])\\s*(ctx\\.getValue\\(\"[^\"]+\"\\))",
//                "$1 $2 ($3 != null ? ((Number)$3).doubleValue() : 0.0)"
//        );
//
//        // 处理两个字段引用之间的运算：ctx.getValue("field1") + ctx.getValue("field2")
//        result = result.replaceAll(
//                "(ctx\\.getValue\\(\"[^\"]+\"\\))\\s*([+\\-*/%])\\s*(ctx\\.getValue\\(\"[^\"]+\"\\))",
//                "($1 != null ? ((Number)$1).doubleValue() : 0.0) $2 ($3 != null ? ((Number)$3).doubleValue() : 0.0)"
//        );
//
//        // 处理比较运算符：ctx.getValue("field") > 10
//        result = result.replaceAll(
//                "(ctx\\.getValue\\(\"[^\"]+\"\\))\\s*([<>]=?|==|!=)\\s*([0-9]+\\.?[0-9]*)",
//                "($1 != null && ((Number)$1).doubleValue() $2 $3)"
//        );
//
//        // 处理两个字段引用之间的比较：ctx.getValue("field1") > ctx.getValue("field2")
//        result = result.replaceAll(
//                "(ctx\\.getValue\\(\"[^\"]+\"\\))\\s*([<>]=?|==|!=)\\s*(ctx\\.getValue\\(\"[^\"]+\"\\))",
//                "($1 != null && $3 != null && ((Number)$1).doubleValue() $2 ((Number)$3).doubleValue())"
//        );

        return result;
    }

    /**
     * 替换运算符
     */
    private String replaceOperators(String expression) {
        // Janino支持标准的Java运算符，所以大部分不需要替换
        // 但可以处理一些特殊的运算符
        return expression
                .replaceAll("\\s+and\\s+", " && ")  // SQL风格的AND
                .replaceAll("\\s+or\\s+", " || ")    // SQL风格的OR
                .replaceAll("\\s+not\\s+", " ! ")    // SQL风格的NOT
                .replaceAll("==", "==")              // 保持相等运算符
                .replaceAll("!=", "!=")              // 保持不等运算符
                .replaceAll("<>", "!=");             // SQL风格的不等
    }

    /**
     * 生成缓存键
     */
    private String generateCacheKey(String expression, Class<?> contextType) {
        return expression.hashCode() + ":" + contextType.getName();
    }

    /**
     * 预编译常用模板
     */
    private void precompileTemplates() {
        // 预编译一些常用表达式模板，提高性能
//        String[] templates = {
//            "ctx.getValue(\"\")",
//            "1 + 1",
//            "ctx.getValue(\"source.field\") == null",
//            "FunctionRegistry.coalesce(ctx.getValue(\"source1.field\"), ctx.getValue(\"source2.field\"))"
//        };
//
//        for (String template : templates) {
//            try {
//                compile(template, ExpressionExecutionContext.class);
//                log.debug("预编译模板: {}", template);
//            } catch (Exception e) {
//                log.warn("预编译模板失败: {}", template, e);
//            }
//        }
    }

    /**
     * 将Java对象转换为Column
     */
    public Column convertToColumn(Object value, Column.Type resultType) {
        if (value == null) {
            return null;
        }

        try {
            switch (resultType) {
                case INT:
                case LONG:
                    if (value instanceof Number) {
                        Long longValue = ((Number) value).longValue();
                        return new LongColumn(longValue);
                    } else if (value instanceof String) {
                        Long longValue = Long.parseLong((String) value);
                        return new LongColumn(longValue);
                    } else {
                        // 尝试转换为Long
                        Long longValue = Long.parseLong(value.toString());
                        return new LongColumn(longValue);
                    }
                case DOUBLE:
                    if (value instanceof Number) {
                        Double doubleValue = ((Number) value).doubleValue();
                        return new DoubleColumn(doubleValue);
                    } else if (value instanceof String) {
                        Double doubleValue = Double.parseDouble((String) value);
                        return new DoubleColumn(doubleValue);
                    } else {
                        Double doubleValue = Double.parseDouble(value.toString());
                        return new DoubleColumn(doubleValue);
                    }
                case STRING:
                    return new StringColumn(value.toString());
                case BOOL:
                    if (value instanceof Boolean) {
                        return new BoolColumn((Boolean) value);
                    } else if (value instanceof String) {
                        String str = ((String) value).toLowerCase();
                        Boolean bool = "true".equals(str) || "1".equals(str) || "yes".equals(str);
                        return new BoolColumn(bool);
                    } else if (value instanceof Number) {
                        Boolean bool = ((Number) value).doubleValue() != 0.0;
                        return new BoolColumn(bool);
                    } else {
                        Boolean bool = Boolean.parseBoolean(value.toString());
                        return new BoolColumn(bool);
                    }
                case DATE:
                    if (value instanceof java.util.Date) {
                        return new DateColumn((java.util.Date) value);
                    } else if (value instanceof String) {
                        // 尝试解析常见日期格式
                        // 简化处理：交给调用方确保日期格式正确
                        throw new IllegalArgumentException("字符串日期转换需要指定格式，请使用TO_DATE函数");
                    } else if (value instanceof Number) {
                        Long timestamp = ((Number) value).longValue();
                        return new DateColumn(new java.util.Date(timestamp));
                    } else {
                        throw new IllegalArgumentException("不支持的日期值类型: " + value.getClass());
                    }
                case BYTES:
                    if (value instanceof byte[]) {
                        return new BytesColumn((byte[]) value);
                    } else if (value instanceof String) {
                        // 将字符串转换为字节数组
                        return new BytesColumn(((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8));
                    } else {
                        throw new IllegalArgumentException("不支持的字节数组值类型: " + value.getClass());
                    }
                case OBJECT:
                    return new ObjectColumn(value);
                case BAD:
                case NULL:
                default:
                    // 默认返回ObjectColumn
                    return new ObjectColumn(value);
            }
        } catch (Exception e) {
            log.error("转换Column失败: value={}, type={}", value, resultType, e);
            return null;
        }
    }

    /**
     * 编译后的表达式包装类
     */
    private static class CompiledExpression {
        final ExpressionEvaluator evaluator;
        final String originalExpression;

        CompiledExpression(ExpressionEvaluator evaluator, String originalExpression) {
            this.evaluator = evaluator;
            this.originalExpression = originalExpression;
        }
    }
}