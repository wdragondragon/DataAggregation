package com.jdragon.aggregation.unstructuredstorage;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;

/**
 * @author hjs
 * @version 1.0
 * @date 2020/7/22 11:45
 */
public class SoapToJSON {

    /**
     * webService soap格式转换
     * @param strSoap
     * @return
     */
    public static JSONObject toJson(String strSoap) {
        JSONObject jsonObject = new JSONObject();
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document document = db.parse(new InputSource(new StringReader(strSoap)));
            NodeList sessions = document.getChildNodes();
            //JSONObject jsonObject = new JSONObject();
            for (int i = 0; i < sessions.getLength(); i++) {
                Node session = sessions.item(i);
                NodeList sessionInfo = session.getChildNodes();
                for (int j = 0; j < sessionInfo.getLength(); j++) {
                    Node node = sessionInfo.item(j);
                    NodeList sessionMeta = node.getChildNodes();
                    for (int k = 0; k < sessionMeta.getLength(); k++) {
                        String keyNode = sessionMeta.item(k).getNodeName();
                        if(keyNode.equals("#text")){
                                  continue;
                        }
                        if(keyNode.contains(":")){
                            String[] keyNodeArr = keyNode.split(":");
                            if(keyNodeArr.length > 1){
                                keyNode = keyNodeArr[1];
                            }else{
                                keyNode = keyNodeArr[0];
                            }
                        }
                        Object jsonObj = JSONObject.parse(sessionMeta.item(k).getTextContent());
                        if(jsonObj instanceof JSONArray){
                            jsonObject.put(keyNode,JSONArray.parseArray(sessionMeta.item(k).getTextContent()));
                        }else if(jsonObj instanceof JSONObject){
                            jsonObject.put(keyNode,JSONObject.parseObject(sessionMeta.item(k).getTextContent()));
                        }else{
                            jsonObject.put(keyNode,sessionMeta.item(k).getTextContent());
                        }
                    }
                }
            }
        } catch (ParserConfigurationException | SAXException | IOException e) {
            System.out.println(e.getMessage());
        }
        return jsonObject;
    }

    public static void main(String[] args) {
        //String webServiceStr = "<ns:getDataResponse xmlns:ns=\"http://service.whole.qx.com\">    <ns:return xmlns:ax21=\"http://model.whole.qx.com/xsd\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"ax21:Result\">        <code>0000</code>        <ax21:content>[{\"TYPHOON_DATE\":null,\"HAZE_SIG\":null,\"COLD_SIG\":null,\"STATION\":\"黄埔\",\"HIGHTEM_DATE\":\"2020-07-14 09:05:00\",\"RAIN_DATE\":null,\"HIGHTEM_SIG\":\"4\",\"COLD_DATE\":null,\"BIGFOG_DATE\":null,\"HAIL_DATE\":null,\"BIGFOG_SIG\":null,\"TDWIND_SIG\":null,\"ROADICE_SIG\":null,\"FIRE_DATE\":null,\"FIRE_SIG\":null,\"RAIN_SIG\":null,\"HAIL_SIG\":null,\"TDWIND_DATE\":null,\"HAZE_DATE\":null,\"TYPHOON_SIG\":null,\"ROADICE_DATE\":null}]</ax21:content>        <ax21:errMsg xsi:nil=\"true\"></ax21:errMsg>    </ns:return></ns:getDataResponse>";
        /*String webServiceStr = "<ns:getDataResponse>    \n" +
                "<ns:return>        \n" +
                "<ax21:code>0000</ax21:code>        \n" +
                "<ax21:content>\n" +
                "    [{\"SCONTENT\":\"多云到阴天，有中雷雨局部大雨气温介于26到32℃之间相对湿度介于65～95%之间吹轻微的东北风\",\"RTIME\":\"2020-07-28 15:20:00\"}]\n" +
                "</ax21:content>        \n" +
                "<ax21:errMsg>\n" +
                "</ax21:errMsg>    \n" +
                "</ns:return>\n" +
                "</ns:getDataResponse>";*/
       /* String webServiceStr = "<ns:getDataResponse xmlns:ns=\"http://service.whole.qx.com\">    \n" +
                "<ns:return xmlns:ax21=\"http://model.whole.qx.com/xsd\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"ax21:Result\">        \n" +
                "<ax21:code>0000</ax21:code>        \n" +
                "<ax21:content>\n" +
                "    [{\"SCONTENT\":\"多云到阴天，有中雷雨局部大雨气温介于26到32℃之间相对湿度介于65～95%之间吹轻微的东北风\",\"RTIME\":\"2020-07-28 15:20:00\"}]\n" +
                "</ax21:content>        \n" +
                "<ax21:errMsg xsi:nil=\"true\"></ax21:errMsg>    \n" +
                "</ns:return>\n" +
                "</ns:getDataResponse>";*/
       String webServiceStr = "<ns:getDataResponse xmlns:ns=\"http://service.whole.qx.com\">\n" +
               "    <ns:return xmlns:ax21=\"http://model.whole.qx.com/xsd\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"ax21:Result\">\n" +
               "        <ax21:code>0000</ax21:code>\n" +
               "        <ax21:content>[{\"TYPHOON_DATE\":null,\"HAZE_SIG\":null,\"COLD_SIG\":null,\"STATION\":\"黄埔\",\"HIGHTEM_DATE\":\"2020-07-14 09:05:00\",\"RAIN_DATE\":null,\"HIGHTEM_SIG\":\"4\",\"COLD_DATE\":null,\"BIGFOG_DATE\":null,\"HAIL_DATE\":null,\"BIGFOG_SIG\":null,\"TDWIND_SIG\":null,\"ROADICE_SIG\":null,\"FIRE_DATE\":null,\"FIRE_SIG\":null,\"RAIN_SIG\":null,\"HAIL_SIG\":null,\"TDWIND_DATE\":null,\"HAZE_DATE\":null,\"TYPHOON_SIG\":null,\"ROADICE_DATE\":null}]</ax21:content>\n" +
               "        <ax21:errMsg xsi:nil=\"true\">\n" +
               "       </ax21:errMsg>\n" +
               "    </ns:return>\n" +
               "</ns:getDataResponse>";
        System.out.println(webServiceStr);
        JSONObject jsonObject = SoapToJSON.toJson(webServiceStr);
        JSONArray jsonArray = jsonObject.getJSONArray("content");
        System.out.println("jsonArray:"+jsonArray);
        System.out.println(jsonObject);
    }
}
