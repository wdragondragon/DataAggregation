package com.jdragon.aggregation.core.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.lang.reflect.Type;

public final class RowCodec {

    private RowCodec() {
    }

    public static String encode(Object value) {
        return JSON.toJSONString(value, SerializerFeature.WriteMapNullValue);
    }

    public static <T> T decode(String json, Class<T> clazz) {
        return JSON.parseObject(json, clazz);
    }

    @SuppressWarnings("unchecked")
    public static <T> T decode(String json, Type type) {
        return (T) JSON.parseObject(json, type);
    }
}
