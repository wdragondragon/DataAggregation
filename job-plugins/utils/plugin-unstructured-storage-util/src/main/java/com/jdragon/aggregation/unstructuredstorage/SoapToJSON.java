package com.jdragon.aggregation.unstructuredstorage;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.Locale;

/**
 * SOAP Envelope 转 JSON 工具，返回 Body 下操作节点的 JSON 结构。
 */
public class SoapToJSON {

    public static JSONObject toJson(String strSoap) {
        try {
            Document document = XmlToJSON.parseSecureDocument(strSoap);
            Element body = findElementByLocalName(document.getDocumentElement(), "Body");
            if (body == null) {
                throw new IllegalArgumentException("SOAP Body is required");
            }
            Element fault = directChild(body, "Fault");
            if (fault != null) {
                JSONObject result = new JSONObject(true);
                JSONObject bodyJson = new JSONObject(true);
                bodyJson.put("Fault", XmlToJSON.elementValue(fault));
                result.put("Body", bodyJson);
                return result;
            }
            JSONObject result = new JSONObject(true);
            NodeList children = body.getChildNodes();
            for (int index = 0; index < children.getLength(); index++) {
                Node node = children.item(index);
                if (node instanceof Element) {
                    XmlToJSON.putRepeated(result, XmlToJSON.localName(node), XmlToJSON.elementValue((Element) node));
                }
            }
            return result;
        } catch (Exception e) {
            throw new IllegalArgumentException("SOAP response parse failed: " + e.getMessage(), e);
        }
    }

    public static String faultMessage(String strSoap) {
        try {
            Document document = XmlToJSON.parseSecureDocument(strSoap);
            Element body = findElementByLocalName(document.getDocumentElement(), "Body");
            Element fault = body == null ? null : directChild(body, "Fault");
            if (fault == null) {
                return null;
            }
            JSONObject faultJson = XmlToJSON.childrenToObject(fault);
            Object faultString = firstPresent(faultJson, "faultstring", "Reason", "Text", "Value");
            Object faultCode = firstPresent(faultJson, "faultcode", "code", "Code", "Value");
            String message = stringifyFaultValue(faultString);
            String code = stringifyFaultValue(faultCode);
            if (message == null || message.trim().isEmpty()) {
                message = fault.getTextContent();
            }
            return (code == null || code.trim().isEmpty()) ? message : code + ": " + message;
        } catch (Exception e) {
            return null;
        }
    }

    private static Element findElementByLocalName(Element root, String localName) {
        if (root == null) {
            return null;
        }
        if (localName.equals(XmlToJSON.localName(root))) {
            return root;
        }
        NodeList children = root.getChildNodes();
        for (int index = 0; index < children.getLength(); index++) {
            Node node = children.item(index);
            if (node instanceof Element) {
                Element found = findElementByLocalName((Element) node, localName);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    private static Element directChild(Element parent, String localName) {
        if (parent == null) {
            return null;
        }
        NodeList children = parent.getChildNodes();
        for (int index = 0; index < children.getLength(); index++) {
            Node node = children.item(index);
            if (node instanceof Element && localName.equals(XmlToJSON.localName(node))) {
                return (Element) node;
            }
        }
        return null;
    }

    private static Object firstPresent(JSONObject object, String... keys) {
        if (object == null) {
            return null;
        }
        for (String key : keys) {
            for (String candidate : object.keySet()) {
                if (candidate != null && candidate.toLowerCase(Locale.ENGLISH).equals(key.toLowerCase(Locale.ENGLISH))) {
                    Object value = object.get(candidate);
                    if (value != null) {
                        return value;
                    }
                }
            }
        }
        return null;
    }

    private static String stringifyFaultValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof JSONObject) {
            JSONObject object = (JSONObject) value;
            Object text = firstPresent(object, "Text", "Value", "faultstring");
            return text == null ? object.toJSONString() : stringifyFaultValue(text);
        }
        if (value instanceof JSONArray) {
            JSONArray array = (JSONArray) value;
            return array.isEmpty() ? "" : stringifyFaultValue(array.get(0));
        }
        return String.valueOf(value).trim();
    }
}
