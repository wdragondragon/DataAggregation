package com.jdragon.aggregation.unstructuredstorage;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class SoapToJSONTest {

    @Test
    public void bodyMustBeDirectSoapNamespaceChildOfEnvelope() {
        String soap = "<soap:Envelope xmlns:soap=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:biz=\"urn:test\">"
                + "<soap:Header><biz:Body><biz:value>wrong</biz:value></biz:Body></soap:Header>"
                + "<soap:Body><biz:Result><biz:value>right</biz:value></biz:Result></soap:Body>"
                + "</soap:Envelope>";

        JSONObject result = SoapToJSON.toJson(soap);

        assertEquals("right", result.getJSONObject("Result").getString("value"));
        assertTrue(result.get("Body") == null);
    }

    @Test
    public void envelopeMustUseSoap11OrSoap12Namespace() {
        String invalid = "<Envelope><Body><Result>wrong</Result></Body></Envelope>";

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> SoapToJSON.toJson(invalid));

        assertTrue(error.getMessage().contains("SOAP response parse failed"));
    }
}
