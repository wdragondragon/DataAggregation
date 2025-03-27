package com.jdragon.aggregation.unstructuredstorage;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import java.util.List;

/**
 * @author hjs
 * @version 1.0
 * @date 2020/7/10 16:50
 */
public class XmlToJSON {
    /**
     * 将xml转换为JSON对象
     * @param xml xml字符串
     * @return
     * @throws Exception
     */
    public static JSONObject toJson(String xml) throws Exception {
        JSONObject jsonObject = new JSONObject();
        Document document = DocumentHelper.parseText(xml);
        //获取根节点元素对象
        Element root = document.getRootElement();
        iterateNodes(root, jsonObject);
        return jsonObject;
    }
    /**
     * 遍历元素
     * @param node 元素
     * @param json 将元素遍历完成之后放的JSON对象
     */
    @SuppressWarnings("unchecked")
    public static void iterateNodes(Element node,JSONObject json){
        //获取当前元素的名称
        String nodeName = node.getName();
        //判断已遍历的JSON中是否已经有了该元素的名称
        if(json.containsKey(nodeName)){
            //该元素在同级下有多个
            Object Object = json.get(nodeName);
            JSONArray array = null;
            if(Object instanceof JSONArray){
                array = (JSONArray) Object;
            }else {
                array = new JSONArray();
                array.add(Object);
            }
            //获取该元素下所有子元素
            List<Element> listElement = node.elements();
            if(listElement.isEmpty()){
                //该元素无子元素，获取元素的值
                String nodeValue = node.getTextTrim();
                array.add(nodeValue);
                json.put(nodeName, array);
                return ;
            }
            //有子元素
            JSONObject newJson = new JSONObject();
            //遍历所有子元素
            for(Element e:listElement){
                //递归
                iterateNodes(e,newJson);
            }
            array.add(newJson);
            json.put(nodeName, array);
            return ;
        }
        //该元素同级下第一次遍历
        //获取该元素下所有子元素
        List<Element> listElement = node.elements();
        if(listElement.isEmpty()){
            //该元素无子元素，获取元素的值
            String nodeValue = node.getTextTrim();
            json.put(nodeName, nodeValue);
            return ;
        }
        //有子节点，新建一个JSONObject来存储该节点下子节点的值
        JSONObject object = new JSONObject();
        //遍历所有一级子节点
        for(Element e:listElement){
            //递归
            iterateNodes(e,object);
        }
        json.put(nodeName, object);
        return ;
    }

    public static void main(String[] args) throws Exception {
        String xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><resp><city>沈阳</city><updatetime>16:01</updatetime><wendu>32</wendu><fengli><![CDATA[2级]]></fengli><shidu>43%</shidu><fengxiang>西北风</fengxiang><sunrise_1>04:22</sunrise_1><sunset_1>19:22</sunset_1><sunrise_2></sunrise_2><sunset_2></sunset_2><yesterday><date_1>9日星期四</date_1><high_1>高温 31℃</high_1><low_1>低温 17℃</low_1><day_1><type_1>多云</type_1><fx_1>南风</fx_1><fl_1><![CDATA[2级]]></fl_1></day_1><night_1><type_1>多云</type_1><fx_1>南风</fx_1><fl_1><![CDATA[2级]]></fl_1></night_1></yesterday><forecast><weather><date>10日星期五</date><high>高温 32℃</high><low>低温 18℃</low><day><type>多云</type><fengxiang>东南风</fengxiang><fengli><![CDATA[2级]]></fengli></day><night><type>多云</type><fengxiang>东南风</fengxiang><fengli><![CDATA[2级]]></fengli></night></weather><weather><date>11日星期六</date><high>高温 33℃</high><low>低温 20℃</low><day><type>多云</type><fengxiang>西南风</fengxiang><fengli><![CDATA[2级]]></fengli></day><night><type>多云</type><fengxiang>西南风</fengxiang><fengli><![CDATA[2级]]></fengli></night></weather><weather><date>12日星期天</date><high>高温 33℃</high><low>低温 21℃</low><day><type>多云</type><fengxiang>西风</fengxiang><fengli><![CDATA[2级]]></fengli></day><night><type>多云</type><fengxiang>西风</fengxiang><fengli><![CDATA[2级]]></fengli></night></weather><weather><date>13日星期一</date><high>高温 32℃</high><low>低温 20℃</low><day><type>多云</type><fengxiang>西南风</fengxiang><fengli><![CDATA[2级]]></fengli></day><night><type>多云</type><fengxiang>西南风</fengxiang><fengli><![CDATA[2级]]></fengli></night></weather><weather><date>14日星期二</date><high>高温 31℃</high><low>低温 20℃</low><day><type>多云</type><fengxiang>西风</fengxiang><fengli><![CDATA[2级]]></fengli></day><night><type>多云</type><fengxiang>西风</fengxiang><fengli><![CDATA[2级]]></fengli></night></weather></forecast><zhishus><zhishu><name>穿衣指数</name><value>清凉夏装</value><detail>天气炎热，建议穿着薄款，透气的衣物。推荐：T恤、裙装、短裤等。</detail></zhishu><zhishu><name>感冒指数</name><value>易发感冒</value><detail>感冒易发期，外出请适当调整衣物，注意补充水分。</detail></zhishu><zhishu><name>紫外线强度</name><value>紫外线较弱</value><detail>紫外线强度弱，外出记得涂防晒霜，避免皮肤受到太阳辐射的危害。</detail></zhishu><zhishu><name>晾晒指数</name><value>较适宜晾晒</value><detail>预计白天没有降水，温度适宜，较适合晾晒.</detail></zhishu><zhishu><name>护肤指数</name><value>清洁护肤</value><detail>空气湿润度高，可以根据皮肤类型选择相应清爽型扶肤品。</detail></zhishu><zhishu><name>户外指数</name><value>较适宜外出</value><detail>天气还可以，预计白天没有降水，适合参加户外活动，适当锻炼身体。</detail></zhishu><zhishu><name>洗车指数</name><value>很适宜洗车</value><detail>天气晴朗，放心洗车吧，今明两天都暂无降水~</detail></zhishu><zhishu><name>污染指数</name><value>轻微污染</value><detail>空气质量良好，污染物浓度低，对健康人群无明显影响，可在户外适当活动。</detail></zhishu></zhishus></resp><!-- 10.42.121.88(10.42.121.88):39955 ; 10.42.161.121:8080 -->";
        JSONObject jsonObject = toJson(xml);
        System.out.println(jsonObject);
    }
}
