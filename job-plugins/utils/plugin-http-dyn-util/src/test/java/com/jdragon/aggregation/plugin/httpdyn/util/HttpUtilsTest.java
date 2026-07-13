package com.jdragon.aggregation.plugin.httpdyn.util;

import org.apache.http.entity.ContentType;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HttpUtilsTest {

    @Test
    public void every2xxStatusShouldBeSuccessful() {
        assertTrue(HttpUtils.isSuccessfulStatus(200));
        assertTrue(HttpUtils.isSuccessfulStatus(201));
        assertTrue(HttpUtils.isSuccessfulStatus(204));
        assertTrue(HttpUtils.isSuccessfulStatus(299));
        assertFalse(HttpUtils.isSuccessfulStatus(199));
        assertFalse(HttpUtils.isSuccessfulStatus(300));
    }

    @Test
    public void logUrlShouldRemoveUserInfoQueryAndFragment() {
        String sanitized = HttpUtils.sanitizeUrlForLog(
                "https://reader:secret@example.test:8443/api/items?token=private#result");

        assertEquals("https://example.test:8443", sanitized);
        assertFalse(sanitized.contains("reader"));
        assertFalse(sanitized.contains("secret"));
        assertFalse(sanitized.contains("token"));
    }

    @Test
    public void requestBodySupportShouldMatchActualRequestClasses() {
        assertTrue(HttpUtils.supportsRequestBody("POST"));
        assertTrue(HttpUtils.supportsRequestBody("put"));
        assertTrue(HttpUtils.supportsRequestBody("PATCH"));
        assertFalse(HttpUtils.supportsRequestBody("GET"));
        assertFalse(HttpUtils.supportsRequestBody("HEAD"));
        assertFalse(HttpUtils.supportsRequestBody("OPTIONS"));
        assertFalse(HttpUtils.supportsRequestBody("TRACE"));
        assertFalse(HttpUtils.supportsRequestBody("DELETE"));
    }

    @Test
    public void responseCharsetShouldUseXmlDeclarationWhenHeaderOmitsCharset() {
        Charset gbk = Charset.forName("GBK");
        byte[] response = "<?xml version=\"1.0\" encoding=\"GBK\"?><root>客户甲</root>".getBytes(gbk);

        assertEquals(gbk, HttpUtils.resolveResponseCharset(ContentType.create("text/xml"), response));
    }

    @Test
    public void responseCharsetShouldUseUtf16BomWhenHeaderOmitsCharset() {
        byte[] content = "<root>客户甲</root>".getBytes(StandardCharsets.UTF_16LE);
        byte[] response = new byte[content.length + 2];
        response[0] = (byte) 0xFF;
        response[1] = (byte) 0xFE;
        System.arraycopy(content, 0, response, 2, content.length);

        assertEquals(StandardCharsets.UTF_16LE,
                HttpUtils.resolveResponseCharset(ContentType.create("text/xml"), response));
    }
}
