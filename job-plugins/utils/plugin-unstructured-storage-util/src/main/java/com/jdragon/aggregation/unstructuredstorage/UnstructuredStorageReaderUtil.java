package com.jdragon.aggregation.unstructuredstorage;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.util.FastJsonMemory;
import com.jdragon.aggregation.commons.util.RegexUtil;
import com.jdragon.aggregation.core.plugin.RecordSender;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class UnstructuredStorageReaderUtil {
    public static List<ColumnEntry> getListColumnEntry(
            Configuration configuration, final String path) {
        List<JSONObject> lists = configuration.getList(path, JSONObject.class);
        if (lists == null) {
            return null;
        }
        List<ColumnEntry> result = new ArrayList<ColumnEntry>();
        for (final JSONObject object : lists) {
            result.add(JSON.parseObject(object.toJSONString(),
                    ColumnEntry.class));
        }
        return result;
    }
    public static JSONArray detailJson(FastJsonMemory json, List<ColumnEntry> columnEntryList) {
        Map<String, List<ColumnEntry>> columnGroupByParent = columnEntryList.stream().collect(Collectors.groupingBy(ColumnEntry::getParentNode));
        List<String> parentNodes = new ArrayList<>(columnGroupByParent.keySet());
        //以那个node为基准取数据。
        Pattern pattern = Pattern.compile("(.*)\\[(\\d+)]");
        String baseParent = "";
        for (String parentNode : parentNodes) {
            String[] parentSplit = parentNode.split("\\.");
            List<String> parentSplitList = new ArrayList<>();
            parentSplitList.add("");
            parentSplitList.addAll(Arrays.asList(parentSplit));
            String temp = "";
            Object o;
            for (String sub : parentSplitList) {
                temp += StringUtils.isBlank(temp) ? sub : "." + sub;
                o = json.get(temp);
                if (o == null) {
                    break;
                }
                Matcher matcher = pattern.matcher(sub);
                boolean m = matcher.find();
                if (StringUtils.isBlank(baseParent) && !m && o instanceof JSONArray) {
                    baseParent = temp;
                }
            }
        }

        //筛选出处于基准node下的数据，这些数据成为对应插入每一行的基准数据
        Map<String, List<ColumnEntry>> baseEntryMap = new HashMap<>();

        //单次数据行，会克隆进基准数据做填充
        Map<String, List<ColumnEntry>> columnEntryMap = new HashMap<>();
        Map<String, Object> dataMap = new HashMap<>();
        for (ColumnEntry columnEntry : columnEntryList) {
            String parentNode = columnEntry.getParentNode();
            if (columnEntry.getParentNode().startsWith(baseParent)) {
                String s = "";
                if (!baseParent.equals(parentNode)) {
                    s = parentNode.replaceAll(RegexUtil.escapeExprSpecialWord(baseParent + "."), "");
                }
                if (!baseEntryMap.containsKey(s)) {
                    baseEntryMap.put(s, new LinkedList<>());
                }
                baseEntryMap.get(s).add(columnEntry);
            } else {
                if (!columnEntryMap.containsKey(parentNode)) {
                    columnEntryMap.put(parentNode, new LinkedList<>());
                }
                columnEntryMap.get(parentNode).add(columnEntry);
                dataMap.put(columnEntry.getParentNode(), json.get(parentNode));
            }
        }

        JSONArray baseJsonArray;
        Object o1 = json.get(baseParent);
        if (o1 instanceof JSONArray) {
            baseJsonArray = (JSONArray) o1;
        } else {
            baseJsonArray = new JSONArray();
            JSONObject baseJsonObject = (JSONObject) o1;
            baseJsonArray.add(baseJsonObject);
        }
        JSONObject item = new JSONObject();

        for (Map.Entry<String, List<ColumnEntry>> entry : columnEntryMap.entrySet()) {
            String parentNode = entry.getKey();
            Object dataNode = dataMap.get(parentNode);
            List<ColumnEntry> columnEntries = entry.getValue();
            for (ColumnEntry columnEntry : columnEntries) {
                if (dataNode instanceof JSONObject) {
                    JSONObject dataObj = (JSONObject) dataNode;
                    item.put(columnEntry.getName(), dataObj.get(columnEntry.getName()));
                }
            }
        }

        JSONArray results = new JSONArray();
        for (Object o : baseJsonArray) {
            FastJsonMemory itemJson = new FastJsonMemory(o);
            JSONObject result = new JSONObject();
            for (Map.Entry<String, List<ColumnEntry>> entry : baseEntryMap.entrySet()) {
                String key = entry.getKey();
                List<ColumnEntry> value = entry.getValue();
                for (ColumnEntry columnEntry : value) {
                    String subDataNode = StringUtils.isBlank(key) ? columnEntry.getName() : key + "." + columnEntry.getName();
                    result.put(columnEntry.getName(), itemJson.get(subDataNode));
                }
            }
            result.putAll(item);
            results.add(result);
        }
        return results;
    }

    /**
     * todo: httpreader 这里是 String name = columnEntry.getName() 与 readJsonDataOneRecord里面差异
     * @param jsonObject
     * @param recordSender
     * @param columnList
     * @return
     */
    public static Record httpReadJsonDataOneRecord(JSONObject jsonObject, RecordSender recordSender, List<ColumnEntry> columnList) {
        Record record = recordSender.createRecord();
        Column columnGenerated = null;
        for (ColumnEntry columnEntry : columnList) {
            String columnType = columnEntry.getType();
            String name = columnEntry.getName();
            Object columnValue = jsonObject.get(name);
            columnGenerated = ValidateUtil.validate(name, columnType, columnValue);
            record.addColumn(columnGenerated);
        }
        return record;
    }
}
