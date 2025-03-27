package com.jdragon.aggregation.httpreader;

import com.alibaba.fastjson.JSON;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 * <p>
 * </p>
 *
 * @author lxs
 * @since 2020/4/13
 */
public class HttpConstant {

    public static String getContentType() {
        return "application/json;charset=utf-8";
    }

    public static String getHeader() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "Content-type");
        map.put("value", "application/json");
        return JSON.toJSONString(map);
    }
}
