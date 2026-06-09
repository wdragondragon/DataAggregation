package com.jdragon.aggregation.unstructuredstorage;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;

/**
 * XML 转 JSON 工具，按 localName 忽略 namespace 前缀。
 */
public class XmlToJSON {

    public static JSONObject toJson(String xml) throws Exception {
        Document document = parseSecureDocument(xml);
        Element root = document.getDocumentElement();
        JSONObject jsonObject = new JSONObject(true);
        jsonObject.put(localName(root), elementValue(root));
        return jsonObject;
    }

    static Document parseSecureDocument(String xml) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setXIncludeAware(false);
        factory.setExpandEntityReferences(false);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        DocumentBuilder builder = factory.newDocumentBuilder();
        return builder.parse(new InputSource(new StringReader(xml)));
    }

    static Object elementValue(Element element) {
        JSONObject children = childrenToObject(element);
        if (!children.isEmpty()) {
            return children;
        }
        return element.getTextContent() == null ? null : element.getTextContent().trim();
    }

    static JSONObject childrenToObject(Element parent) {
        JSONObject result = new JSONObject(true);
        NodeList children = parent.getChildNodes();
        for (int index = 0; index < children.getLength(); index++) {
            Node node = children.item(index);
            if (node instanceof Element) {
                putRepeated(result, localName(node), elementValue((Element) node));
            }
        }
        return result;
    }

    static void putRepeated(JSONObject target, String key, Object value) {
        if (!target.containsKey(key)) {
            target.put(key, value);
            return;
        }
        Object existing = target.get(key);
        JSONArray array;
        if (existing instanceof JSONArray) {
            array = (JSONArray) existing;
        } else {
            array = new JSONArray();
            array.add(existing);
        }
        array.add(value);
        target.put(key, array);
    }

    static String localName(Node node) {
        String localName = node.getLocalName();
        return localName == null || localName.trim().isEmpty() ? node.getNodeName() : localName;
    }
}
