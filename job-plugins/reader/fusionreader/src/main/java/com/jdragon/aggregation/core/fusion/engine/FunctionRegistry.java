package com.jdragon.aggregation.core.fusion.engine;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 函数注册表
 * 管理表达式引擎的内置函数
 */
@Slf4j
public class FunctionRegistry {

    private static final Map<String, Method> functionMap = new HashMap<>();
    private static final Map<String, String> functionDescriptions = new HashMap<>();

    static {
        registerFunctions();
    }

    /**
     * 注册所有内置函数
     */
    private static void registerFunctions() {
        // ========== 字符串函数 ==========
        registerStringFunctions();

        // ========== 数值函数 ==========
        registerMathFunctions();

        // ========== 日期函数 ==========
        registerDateFunctions();

        // ========== 逻辑函数 ==========
        registerLogicalFunctions();

        // ========== 聚合函数 ==========
        registerAggregateFunctions();

        // ========== 类型转换函数 ==========
        registerConversionFunctions();
    }

    /**
     * 字符串函数
     */
    private static void registerStringFunctions() {
        register("CONCAT", FunctionRegistry.class, "concat", "连接多个字符串: CONCAT(str1, str2, ...)", String[].class);

        register("SUBSTR", FunctionRegistry.class, "substr", "截取子字符串: SUBSTR(str, start, length)", String.class, Integer.class, Integer.class);

        register("UPPER", FunctionRegistry.class, "upper", "转换为大写: UPPER(str)", String.class);

        register("LOWER", FunctionRegistry.class, "lower", "转换为小写: LOWER(str)", String.class);

        register("TRIM", FunctionRegistry.class, "trim", "去除首尾空格: TRIM(str)", String.class);

        register("LENGTH", FunctionRegistry.class, "length", "获取字符串长度: LENGTH(str)", String.class);

        register("REPLACE", FunctionRegistry.class, "replace", "替换字符串: REPLACE(str, old, new)", String.class, String.class, String.class);

        register("LEFT", FunctionRegistry.class, "left", "从左截取: LEFT(str, length)", String.class, Integer.class);

        register("RIGHT", FunctionRegistry.class, "right", "从右截取: RIGHT(str, length)", String.class, Integer.class);
    }

    /**
     * 数学函数
     */
    private static void registerMathFunctions() {
        register("ROUND", FunctionRegistry.class, "round", "四舍五入: ROUND(num, decimals)", Double.class, Integer.class);

        register("CEIL", FunctionRegistry.class, "ceil", "向上取整: CEIL(num)", Double.class);

        register("FLOOR", FunctionRegistry.class, "floor", "向下取整: FLOOR(num)", Double.class);

        register("ABS", FunctionRegistry.class, "abs", "绝对值: ABS(num)", Double.class);

        register("POW", FunctionRegistry.class, "pow", "幂运算: POW(base, exponent)", Double.class, Double.class);

        register("SQRT", FunctionRegistry.class, "sqrt", "平方根: SQRT(num)", Double.class);

        register("LOG", FunctionRegistry.class, "log", "自然对数: LOG(num)", Double.class);

        register("LOG10", FunctionRegistry.class, "log10", "常用对数: LOG10(num)", Double.class);

        register("EXP", FunctionRegistry.class, "exp", "指数函数: EXP(num)", Double.class);

        register("MOD", FunctionRegistry.class, "mod", "取模运算: MOD(num, divisor)", Double.class, Double.class);

        register("MAX", FunctionRegistry.class, "max", "最大值: MAX(num1, num2, ...)", Double[].class);

        register("MIN", FunctionRegistry.class, "min", "最小值: MIN(num1, num2, ...)", Double[].class);

        register("SUM", FunctionRegistry.class, "sum", "求和: SUM(num1, num2, ...)", Double[].class);

        register("AVG", FunctionRegistry.class, "avg", "平均值: AVG(num1, num2, ...)", Double[].class);
    }

    /**
     * 日期函数
     */
    private static void registerDateFunctions() {
        register("DATE_FORMAT", FunctionRegistry.class, "dateFormat", "格式化日期: DATE_FORMAT(date, format)", Date.class, String.class);

        register("DATE_ADD", FunctionRegistry.class, "dateAdd", "日期加减: DATE_ADD(date, amount, unit)", Date.class, Integer.class, String.class);

        register("NOW", FunctionRegistry.class, "now", "当前时间: NOW()");

        register("DATEDIFF", FunctionRegistry.class, "dateDiff", "日期差: DATEDIFF(date1, date2, unit)", Date.class, Date.class, String.class);

        register("YEAR", FunctionRegistry.class, "year", "获取年份: YEAR(date)", Date.class);

        register("MONTH", FunctionRegistry.class, "month", "获取月份: MONTH(date)", Date.class);

        register("DAY", FunctionRegistry.class, "day", "获取日: DAY(date)", Date.class);

        register("HOUR", FunctionRegistry.class, "hour", "获取小时: HOUR(date)", Date.class);

        register("MINUTE", FunctionRegistry.class, "minute", "获取分钟: MINUTE(date)", Date.class);

        register("SECOND", FunctionRegistry.class, "second", "获取秒: SECOND(date)", Date.class);
    }

    /**
     * 逻辑函数
     */
    private static void registerLogicalFunctions() {
        register("IF", FunctionRegistry.class, "ifExpr", "条件判断: IF(condition, trueValue, falseValue)", Boolean.class, Object.class, Object.class);

        register("COALESCE", FunctionRegistry.class, "coalesce", "返回第一个非空值: COALESCE(val1, val2, ...)", Object[].class);

        register("NULLIF", FunctionRegistry.class, "nullif", "相等返回null: NULLIF(val1, val2)", Object.class, Object.class);

        register("CASE", FunctionRegistry.class, "caseExpr", "多条件判断: CASE(condition1, value1, condition2, value2, ..., elseValue)", Object[].class);

        register("ISNULL", FunctionRegistry.class, "isNull", "判断是否为空: ISNULL(val)", Object.class);

        register("ISNOTNULL", FunctionRegistry.class, "isNotNull", "判断是否非空: ISNOTNULL(val)", Object.class);

        register("IFNULL", FunctionRegistry.class, "ifNull", "判断是否非空: IFNULL(val1, val2)", Object.class, Object.class);
    }

    /**
     * 聚合函数（在表达式上下文中简化实现）
     */
    private static void registerAggregateFunctions() {
        // 注意：这些函数在表达式引擎中通常需要特殊处理
        // 这里提供简化版本，仅用于演示
    }

    /**
     * 类型转换函数
     */
    private static void registerConversionFunctions() {
        register("TO_NUMBER", FunctionRegistry.class, "toNumber", "转换为数字: TO_NUMBER(val)", Object.class);

        register("TO_INTEGER", FunctionRegistry.class, "toInteger", "转换为整数: TO_INTEGER(val)", Object.class);

        register("TO_STRING", FunctionRegistry.class, "toString", "转换为字符串: TO_STRING(val)", Object.class);

        register("TO_DATE", FunctionRegistry.class, "toDate", "转换为日期: TO_DATE(val, format)", Object.class, String.class);

        register("TO_BOOLEAN", FunctionRegistry.class, "toBoolean", "转换为布尔值: TO_BOOLEAN(val)", Object.class);
    }

    /**
     * 注册函数
     */
    private static void register(String name, Class<?> clazz, String methodName, Class<?>... paramTypes) {
        try {
            Method method = clazz.getDeclaredMethod(methodName, paramTypes);
            functionMap.put(name, method);
        } catch (NoSuchMethodException e) {
            log.warn("无法注册函数 {}.{}: {}", clazz.getSimpleName(), methodName, e.getMessage());
        }
    }

    private static void register(String name, Class<?> clazz, String methodName, String description, Class<?>... paramTypes) {
        register(name.toUpperCase(), clazz, methodName, paramTypes);
        functionDescriptions.put(name.toUpperCase(), description);
    }

    /**
     * 获取函数方法
     */
    public static Method getFunction(String name) {
        return functionMap.get(name.toUpperCase());
    }

    /**
     * 获取函数描述
     */
    public static String getFunctionDescription(String name) {
        return functionDescriptions.get(name.toUpperCase());
    }

    /**
     * 获取所有注册的函数名
     */
    public static String[] getRegisteredFunctions() {
        return functionMap.keySet().toArray(new String[0]);
    }

    /**
     * 检查函数是否存在
     */
    public static boolean hasFunction(String name) {
        return functionMap.containsKey(name.toUpperCase());
    }

    // ========== 函数实现 ==========

    // 字符串函数实现
    public static String concat(String... strs) {
        StringBuilder sb = new StringBuilder();
        for (String str : strs) {
            if (str != null) {
                sb.append(str);
            }
        }
        return sb.toString();
    }

    public static String substr(String str, Integer start, Integer length) {
        if (str == null || start == null) return null;
        if (start < 0) start = 0;
        if (length == null || start + length > str.length()) {
            return str.substring(start);
        }
        return str.substring(start, start + length);
    }

    public static String upper(String str) {
        return str != null ? str.toUpperCase() : null;
    }

    public static String lower(String str) {
        return str != null ? str.toLowerCase() : null;
    }

    public static String trim(String str) {
        return str != null ? str.trim() : null;
    }

    public static Integer length(String str) {
        return str != null ? str.length() : 0;
    }

    public static String replace(String str, String oldStr, String newStr) {
        return str != null && oldStr != null && newStr != null ?
                str.replace(oldStr, newStr) : str;
    }

    public static String left(String str, Integer length) {
        if (str == null || length == null || length <= 0) return "";
        if (length >= str.length()) return str;
        return str.substring(0, length);
    }

    public static String right(String str, Integer length) {
        if (str == null || length == null || length <= 0) return "";
        if (length >= str.length()) return str;
        return str.substring(str.length() - length);
    }

    // 数学函数实现
    public static Double round(Double num, Integer decimals) {
        if (num == null) return null;
        if (decimals == null) decimals = 0;
        BigDecimal bd = BigDecimal.valueOf(num);
        bd = bd.setScale(decimals, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }

    public static Double ceil(Double num) {
        return num != null ? Math.ceil(num) : null;
    }

    public static Double floor(Double num) {
        return num != null ? Math.floor(num) : null;
    }

    public static Double abs(Double num) {
        return num != null ? Math.abs(num) : null;
    }

    public static Double pow(Double base, Double exponent) {
        return base != null && exponent != null ? Math.pow(base, exponent) : null;
    }

    public static Double sqrt(Double num) {
        return num != null && num >= 0 ? Math.sqrt(num) : null;
    }

    public static Double log(Double num) {
        return num != null && num > 0 ? Math.log(num) : null;
    }

    public static Double log10(Double num) {
        return num != null && num > 0 ? Math.log10(num) : null;
    }

    public static Double exp(Double num) {
        return num != null ? Math.exp(num) : null;
    }

    public static Double mod(Double num, Double divisor) {
        return num != null && divisor != null && divisor != 0 ? num % divisor : null;
    }

    public static Double max(Double... nums) {
        if (nums == null || nums.length == 0) return null;
        Double max = nums[0];
        for (Double num : nums) {
            if (num != null && (max == null || num > max)) {
                max = num;
            }
        }
        return max;
    }

    public static Double min(Double... nums) {
        if (nums == null || nums.length == 0) return null;
        Double min = nums[0];
        for (Double num : nums) {
            if (num != null && (min == null || num < min)) {
                min = num;
            }
        }
        return min;
    }

    public static Double sum(Double... nums) {
        if (nums == null) return null;
        Double sum = 0.0;
        for (Double num : nums) {
            if (num != null) {
                sum += num;
            }
        }
        return sum;
    }

    public static Double avg(Double... nums) {
        if (nums == null) return null;
        Double sum = 0.0;
        int count = 0;
        for (Double num : nums) {
            if (num != null) {
                sum += num;
                count++;
            }
        }
        return count > 0 ? sum / count : null;
    }

    // 日期函数实现
    public static String dateFormat(Date date, String format) {
        if (date == null || format == null) return null;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            return sdf.format(date);
        } catch (Exception e) {
            log.warn("日期格式化失败: date={}, format={}", date, format, e);
            return null;
        }
    }

    public static Date dateAdd(Date date, Integer amount, String unit) {
        if (date == null || amount == null || unit == null) return null;

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);

        switch (unit.toUpperCase()) {
            case "YEAR":
            case "YEARS":
                calendar.add(Calendar.YEAR, amount);
                break;
            case "MONTH":
            case "MONTHS":
                calendar.add(Calendar.MONTH, amount);
                break;
            case "DAY":
            case "DAYS":
                calendar.add(Calendar.DAY_OF_MONTH, amount);
                break;
            case "HOUR":
            case "HOURS":
                calendar.add(Calendar.HOUR, amount);
                break;
            case "MINUTE":
            case "MINUTES":
                calendar.add(Calendar.MINUTE, amount);
                break;
            case "SECOND":
            case "SECONDS":
                calendar.add(Calendar.SECOND, amount);
                break;
            default:
                log.warn("不支持的日期单位: {}", unit);
                return null;
        }

        return calendar.getTime();
    }

    public static Date now() {
        return new Date();
    }

    public static Long dateDiff(Date date1, Date date2, String unit) {
        if (date1 == null || date2 == null || unit == null) return null;

        long diffMillis = date1.getTime() - date2.getTime();

        switch (unit.toUpperCase()) {
            case "MILLISECOND":
            case "MILLISECONDS":
                return diffMillis;
            case "SECOND":
            case "SECONDS":
                return diffMillis / 1000;
            case "MINUTE":
            case "MINUTES":
                return diffMillis / (1000 * 60);
            case "HOUR":
            case "HOURS":
                return diffMillis / (1000 * 60 * 60);
            case "DAY":
            case "DAYS":
                return diffMillis / (1000 * 60 * 60 * 24);
            default:
                log.warn("不支持的日期差单位: {}", unit);
                return null;
        }
    }

    public static Integer year(Date date) {
        if (date == null) return null;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.YEAR);
    }

    public static Integer month(Date date) {
        if (date == null) return null;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.MONTH) + 1; // Calendar.MONTH是从0开始的
    }

    public static Integer day(Date date) {
        if (date == null) return null;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.DAY_OF_MONTH);
    }

    public static Integer hour(Date date) {
        if (date == null) return null;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.HOUR_OF_DAY);
    }

    public static Integer minute(Date date) {
        if (date == null) return null;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.MINUTE);
    }

    public static Integer second(Date date) {
        if (date == null) return null;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.SECOND);
    }

    // 逻辑函数实现
    public static Object ifExpr(Boolean condition, Object trueValue, Object falseValue) {
        return condition != null && condition ? trueValue : falseValue;
    }

    public static Object coalesce(Object... values) {
        if (values == null) return null;
        for (Object value : values) {
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    public static Object nullif(Object val1, Object val2) {
        if (val1 == null || val2 == null) return val1;
        return val1.equals(val2) ? null : val1;
    }

    public static Object caseExpr(Object... args) {
        if (args == null || args.length < 3) return null;

        // 参数格式: condition1, value1, condition2, value2, ..., elseValue
        int pairs = (args.length - 1) / 2;

        for (int i = 0; i < pairs; i++) {
            Object condition = args[i * 2];
            Object value = args[i * 2 + 1];

            if (condition instanceof Boolean && (Boolean) condition) {
                return value;
            }
        }

        // 返回else值
        return args[args.length - 1];
    }

    public static Boolean isNull(Object val) {
        return val == null;
    }

    public static Boolean isNotNull(Object val) {
        return val != null;
    }

    public static Object ifNull(Object val1, Object val2) {
        return val1 == null ? val2 : val1;
    }

    // 类型转换函数实现
    public static Integer toInteger(Object val) {
        if (val == null) return null;

        if (val instanceof Number) {
            return ((Number) val).intValue();
        }

        if (val instanceof String) {
            try {
                return Integer.parseInt((String) val);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    // 类型转换函数实现
    public static Double toNumber(Object val) {
        if (val == null) return null;

        if (val instanceof Number) {
            return ((Number) val).doubleValue();
        }

        if (val instanceof String) {
            try {
                return Double.parseDouble((String) val);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        if (val instanceof Boolean) {
            return (Boolean) val ? 1.0 : 0.0;
        }

        return null;
    }

    public static String toString(Object val) {
        return val != null ? val.toString() : null;
    }

    public static Date toDate(Object val, String format) {
        if (val == null || format == null) return null;

        if (val instanceof Date) {
            return (Date) val;
        }

        if (val instanceof String) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(format);
                return sdf.parse((String) val);
            } catch (Exception e) {
                log.warn("日期转换失败: val={}, format={}", val, format, e);
                return null;
            }
        }

        if (val instanceof Number) {
            return new Date(((Number) val).longValue());
        }

        return null;
    }

    public static Boolean toBoolean(Object val) {
        if (val == null) return null;

        if (val instanceof Boolean) {
            return (Boolean) val;
        }

        if (val instanceof String) {
            String str = ((String) val).toLowerCase();
            return "true".equals(str) || "1".equals(str) || "yes".equals(str) || "y".equals(str);
        }

        if (val instanceof Number) {
            return ((Number) val).doubleValue() != 0.0;
        }

        return null;
    }
}