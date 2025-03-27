package com.jdragon.aggregation.plugin.httpdyn;

import java.util.Map;

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
     * @return 返回
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
     * @return 返回
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
     * @return 返回
     */
    public static void replaceOffset(Map<String, String> httpContent, String offset) {
        for (String key : httpContent.keySet()) {
            String oldValue = httpContent.get(key);
            oldValue = oldValue.replaceAll(OFFSET_DYN_NAME, offset);
            httpContent.put(key, oldValue);
        }
    }

}
