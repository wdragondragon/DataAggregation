package com.jdragon.aggregation.writer.http;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.Configuration;

import java.util.ArrayList;
import java.util.List;

public class ColumnEntry {
    private Integer index;
    private String type;
    private String name;

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public static List<ColumnEntry> getListColumnEntry(Configuration configuration, String path) {
        List<JSONObject> list = configuration.getList(path, JSONObject.class);
        if (list == null) {
            return new ArrayList<>();
        }
        List<ColumnEntry> result = new ArrayList<>();
        for (JSONObject object : list) {
            result.add(JSON.parseObject(object.toJSONString(), ColumnEntry.class));
        }
        return result;
    }
}
