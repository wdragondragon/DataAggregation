package com.jdragon.aggregation.plugin.httpdyn;

import com.jdragon.aggregation.commons.util.RegexUtil;
import com.jdragon.aggregation.plugin.httpdyn.entity.DynPatternParams;
import com.jdragon.aggregation.plugin.httpdyn.enums.HttpDynConfigEnum;
import com.jdragon.aggregation.plugin.httpdyn.util.ReflectionUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class HttpDynColumnExecuteHandler {

    /**
     * pageNum 动态特殊处理字符名称
     */
    private static final String PAGE_DYN_NAME = "\"\\{dyn_page}\"|\\{dyn_page}";

    /**
     * pageSize 动态特殊处理字符名称
     */
    private static final String PAGE_SIZE_DYN_NAME = "\"\\{dyn_pageSize}\"|\\{dyn_pageSize}";

    /**
     * offset 动态特殊处理字符名称
     */
    private static final String OFFSET_DYN_NAME = "\"\\{dyn_offset}\"|\\{dyn_offset}";


    /**
     * 替换当前页
     *
     * @param httpContent 旧值
     * @param page        当前页
     */
    public static void replacePage(Map<String, String> httpContent, String page) {
        for (String key : httpContent.keySet()) {
            String oldValue = httpContent.get(key);
            oldValue = oldValue.replaceAll(PAGE_DYN_NAME, page);
            httpContent.put(key, oldValue);
        }
    }

    /**
     * 替换页面大小
     *
     * @param httpContent 旧值
     * @param pageSize    页面大小
     */
    public static void replacePageSize(Map<String, String> httpContent, String pageSize) {
        for (String key : httpContent.keySet()) {
            String oldValue = httpContent.get(key);
            oldValue = oldValue.replaceAll(PAGE_SIZE_DYN_NAME, pageSize);
            httpContent.put(key, oldValue);
        }
    }

    /**
     * 替换页面大小
     *
     * @param httpContent 旧值
     * @param offset      偏移量
     */
    public static void replaceOffset(Map<String, String> httpContent, String offset) {
        for (String key : httpContent.keySet()) {
            String oldValue = httpContent.get(key);
            oldValue = oldValue.replaceAll(OFFSET_DYN_NAME, offset);
            httpContent.put(key, oldValue);
        }
    }

    /**
     * 获取符合的字符串
     *
     * @param str 字符串
     * @param reg 正则
     * @return 返回set
     */
    public static Set<String> getMatchSet(String str, String reg) {
        Set<String> result = new HashSet<>();
        Pattern pattern = Pattern.compile(reg);
        Matcher matcher = pattern.matcher(str);
        while (matcher.find()) {
            result.add(matcher.group());
        }
        return result;
    }


    /**
     * 获取匹配所有的结果
     *
     * @param str   字符串
     * @param reg   正则
     * @param group 需要返回的group
     */
    public static Set<List<String>> getMatchSet(String str, String reg, Integer group) {
        Set<List<String>> result = new HashSet<>();
        Pattern pattern = Pattern.compile(reg);
        Matcher matcher = pattern.matcher(str);
        while (matcher.find()) {
            List<String> matchList = new LinkedList<>();
            if (group == null) {
                int groupCount = matcher.groupCount();
                for (int i = 1; i <= groupCount; i++) {
                    matchList.add(matcher.group(i));
                }
            } else {
                matchList.add(matcher.group(group));
            }
            result.add(matchList);
        }
        return result;
    }

    /**
     * @param str 传入需要解析的字符
     * @return 返回set, 去重
     */
    public static List<DynPatternParams> getHttpDynWithParamsMatchSet(String str, String reg) {
        List<DynPatternParams> result = new ArrayList<>();
        Pattern pattern = Pattern.compile(reg);
        Matcher matcher = pattern.matcher(str);
        while (matcher.find()) {
            DynPatternParams patternParams = new DynPatternParams();
            patternParams.setOriginalParams(matcher.group());
            patternParams.setExecuteCode(matcher.group(1) + "}");
            //待会测试有括号的，但是不带参数的情况
            patternParams.setParamStrArr(matcher.group(2).split(","));
            result.add(patternParams);
        }
        return result;
    }

    /**
     * @param httpContent 需要动态生成的参数
     */
    public static void replaceStaticVal(Map<String, String> httpContent) {
        for (String key : httpContent.keySet()) {
            String str = httpContent.get(key);
            String prefix = "dyn";
            int i = 1;
            Set<String> matchSet;
            do {
                String format;
                if (i == 1) {
                    format = String.format("<%s_(.*?)>", prefix);
                } else {
                    format = String.format("<%s%s_(.*?)>", prefix, i);
                }
                matchSet = getMatchSet(str, format);
                for (String dynStr : matchSet) {
                    String className = HttpDynConfigEnum.getClassNameByCode(dynStr
                            .replaceAll("\\{", "<")
                            .replaceAll("}", ">")
                            .replaceAll("<" + prefix + prefix, "<" + prefix));
                    if (StringUtils.isNotBlank(className)) {
                        Object reflectObject = ReflectionUtils.objectForName(className);
                        if (reflectObject != null) {
                            BaseHttpDynExecutor baseHttpDynExecutor = (BaseHttpDynExecutor) reflectObject;
                            String exeResultStr = baseHttpDynExecutor.execute();
                            str = str.replaceAll(RegexUtil.escapeExprSpecialWord(dynStr), exeResultStr);
                        }
                    }
                }
                i++;
            } while (!matchSet.isEmpty());
            //遍历获取带参数的动态值
            i = 1;
            List<DynPatternParams> httpDynParams;
            do {
                String format;
                if (i == 1) {
                    format = String.format("(<%s_.*?)\\((.*?)\\)>", prefix);
                } else {
                    format = String.format("(<%s%s_.*?)\\((.*?)\\)>", prefix, i);
                }
                httpDynParams = getHttpDynWithParamsMatchSet(str, format);
                for (DynPatternParams httpDynParam : httpDynParams) {
                    String className = HttpDynConfigEnum.getClassNameByCode(httpDynParam.getExecuteCode()
                            .replaceAll("\\{", "<")
                            .replaceAll("}", ">")
                            .replaceAll("<" + prefix + prefix, "<" + prefix));
                    if (StringUtils.isNotBlank(className)) {
                        Object reflectObject = ReflectionUtils.objectForName(className);
                        if (reflectObject != null) {
                            BaseHttpDynExecutor baseHttpDynExecutor = (BaseHttpDynExecutor) reflectObject;
                            String exeResultStr = baseHttpDynExecutor.execute((Object[]) httpDynParam.getParamStrArr());
                            str = str.replaceAll(RegexUtil.escapeExprSpecialWord(httpDynParam.getOriginalParams()), exeResultStr);
                        }
                    }
                }
                i++;
            } while (!httpDynParams.isEmpty());
            httpContent.put(key, str);
        }
    }

    /**
     * @param httpContent 需要动态生成的参数
     */
    public static void replaceDynVal(Map<String, String> httpContent) {
        //替换httpContent中的上下文
        Map<String, List<String>> keyMap = new LinkedHashMap<>(3);
        for (String key : httpContent.keySet()) {
            String str = httpContent.get(key);
            Set<List<String>> matchSet = getMatchSet(str, "\\{%(header|body|params)%}", 1);
            List<String> matchType = matchSet.stream().map(e -> e.get(0)).collect(Collectors.toList());
            keyMap.put(key, matchType);
        }
        //排序
        List<String> keyList = keyMap.entrySet().stream()
                .sorted((o1, o2) -> {
                    if (o2.getValue().contains(o1.getKey())) {
                        return -1;
                    } else {
                        return o1.getValue().size() - o2.getValue().size();
                    }
                })
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        String first = keyList.get(0);
        String second = keyList.get(1);
        String three = keyList.get(2);
        if (!keyMap.get(first).isEmpty() ||
                (keyMap.get(second).contains(three) && keyMap.get(three).contains(second))) {
            log.warn("http动态请求标签存在循环依赖，会存在标签并未处理的情况，如无需处理请忽视");
        }

        //开始替换动态参数
        for (String key : keyList) {
            String str = httpContent.get(key);
            String prefix = "dyn";
            int i = 1;
            Set<String> matchSet;
            do {
                String format;
                String suffix = "";
                if (i == 1) {
                    format = String.format("\\{%s_(.*?)\\}", prefix);
                } else {
                    format = String.format("\\{%s%s_(.*?)\\}", prefix, i);
                    suffix = String.valueOf(i);
                }
                matchSet = getMatchSet(str, format);
                //存放执行方法后对应的值
                for (String dynStr : matchSet) {
                    String className = HttpDynConfigEnum.getClassNameByCode(dynStr
                            .replaceAll("\\{", "<")
                            .replaceAll("}", ">")
                            .replaceAll("<" + prefix + suffix, "<" + prefix));
                    if (StringUtils.isNotBlank(className)) {
                        Object reflectObject = ReflectionUtils.objectForName(className);
                        if (reflectObject != null) {
                            BaseHttpDynExecutor baseHttpDynExecutor = (BaseHttpDynExecutor) reflectObject;
                            String exeResultStr = baseHttpDynExecutor.execute();
                            str = str.replaceAll(RegexUtil.escapeExprSpecialWord(dynStr), exeResultStr);
                            httpContent.put(key, str);
                        }
                    }
                }
                i++;
            } while (!matchSet.isEmpty());
            //遍历获取带参数的动态值
            i = 1;
            List<DynPatternParams> httpDynParams;
            do {
                String format;
                String suffix = "";
                if (i == 1) {
                    format = String.format("(\\{%s_.*?)\\((.*?)\\)\\}", prefix);
                } else {
                    format = String.format("(\\{%s%s_.*?)\\((.*?)\\)\\}", prefix, i);
                    suffix = String.valueOf(i);
                }
                httpDynParams = getHttpDynWithParamsMatchSet(str, format);
                for (DynPatternParams httpDynParam : httpDynParams) {
                    String className = HttpDynConfigEnum.getClassNameByCode(httpDynParam.getExecuteCode()
                            .replaceAll("\\{", "<")
                            .replaceAll("}", ">")
                            .replaceAll("<" + prefix + suffix, "<" + prefix));
                    if (StringUtils.isNotBlank(className)) {
                        Object reflectObject = ReflectionUtils.objectForName(className);
                        if (reflectObject != null) {
                            BaseHttpDynExecutor baseHttpDynExecutor = (BaseHttpDynExecutor) reflectObject;
                            String[] paramStrArr = httpDynParam.getParamStrArr();
                            //替换header,body,params
                            for (int i1 = 0; i1 < paramStrArr.length; i1++) {
                                List<String> depends = keyMap.get(key);
                                for (String depend : depends) {
                                    paramStrArr[i1] = paramStrArr[i1].replaceAll("\\{%(" + depend + ")%}", httpContent.get(depend));
                                }
                            }
                            log.info("执行动态参数：{}，参数：{}", httpDynParam.getExecuteCode(), paramStrArr);
                            String exeResultStr = baseHttpDynExecutor.execute((Object[]) paramStrArr);
                            str = str.replaceAll(RegexUtil.escapeExprSpecialWord(httpDynParam.getOriginalParams()), exeResultStr);
                            httpContent.put(key, str);
                        }
                    }
                }
                i++;
            } while (!httpDynParams.isEmpty());
            httpContent.put(key, str);
        }
    }

}
