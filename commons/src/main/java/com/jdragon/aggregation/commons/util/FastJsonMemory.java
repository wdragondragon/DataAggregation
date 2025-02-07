package com.jdragon.aggregation.commons.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author JDragon
 * @Date 2021.03.16 上午 11:25
 * @Email 1061917196@qq.com
 * @Des: 用来对json的初始化与寄存，对json链式操作的优化
 */
public class FastJsonMemory {
    private JSON json;

    public FastJsonMemory(JSON json) {
        this.json = JSONObject.parseObject(JSONObject.toJSONString(json));
    }

    public FastJsonMemory(String string) {
        this.json = JSON.parseObject(string, JSON.class);
    }

    public FastJsonMemory(Object o) {
        this.json = JSON.parseObject(JSON.toJSONString(o), JSON.class);
    }

    public <T> T get(String path, Class<T> clazz) {
        Object object = get(path);
        return JSON.parseObject(JSON.toJSONString(object), clazz);
    }

    public <T> T get(String path, TypeReference<T> type) {
        Object object = get(path);
        return JSON.parseObject(JSON.toJSONString(object), type);
    }

    public Object get(String path) {
        if (StringUtils.isBlank(path)) {
            return json;
        }
        String[] split = path.split("\\.");
        Object object = json;
        Pattern r = Pattern.compile("(.*)\\[(\\d+)]");
        for (String s : split) {
            String key = s;
            Integer index = null;
            Matcher m = r.matcher(s);
            if (m.find()) {
                key = m.group(1);
                index = Integer.parseInt(m.group(2));
            }
            if (object instanceof JSONObject) {
                object = ((JSONObject) object).get(key);
            } else if (index != null && StringUtils.isBlank(key) && object instanceof JSONArray) {
                object = ((JSONArray) object).getJSONObject(index);
            } else {
                return null;
            }
            if (index != null && object instanceof JSONArray) {
                object = ((JSONArray) object).get(index);
            }
        }
        return object;
    }

    @Override
    public String toString() {
        return json.toJSONString();
    }
}
