package com.jdragon.aggregation.unstructuredstorage;

import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.element.Column;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ValidateUtilTest {

    @Test
    public void decimalShouldPreservePlainTextPrecision() {
        Column column = ValidateUtil.validate("amount", "DECIMAL", "10.1250");

        assertEquals("10.1250", column.asString());
        assertEquals("10.1250", column.asBigDecimal().toPlainString());
    }

    @Test
    public void numberWithDecimalTextShouldPreserveFraction() {
        Column column = ValidateUtil.validate("amount", "NUMBER", "20.2500");

        assertEquals("20.2500", column.asString());
        assertEquals("20.2500", column.asBigDecimal().toPlainString());
    }

    @Test
    public void xmlParserShouldReadXsiNilAsNull() throws Exception {
        JSONObject json = XmlToJSON.toJson("<root xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><name xsi:nil=\"true\"/></root>");

        assertNull(json.getJSONObject("root").get("name"));
    }
}
