package com.jdragon.aggregation.kafka;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.Configuration;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author JDragon
 * @Date 2021.12.02 下午 9:35
 * @Email 1061917196@qq.com
 * @Des:
 */
@Data
public class ColumnEntry {
    private Integer index;

    private String type;

    private String name;

    public static List<ColumnEntry> getListColumnEntry(
            Configuration configuration, final String path) {
        List<JSONObject> lists = configuration.getList(path, JSONObject.class);
        if (lists == null) {
            return new ArrayList<>();
        }
        List<ColumnEntry> result = new ArrayList<>();
        for (final JSONObject object : lists) {
            result.add(JSON.parseObject(object.toJSONString(),
                    ColumnEntry.class));
        }
        return result;
    }
}
