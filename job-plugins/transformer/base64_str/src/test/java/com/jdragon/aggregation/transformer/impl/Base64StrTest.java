package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.BytesColumn;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.LongColumn;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.transformer.TransformerErrorCode;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.*;

public class Base64StrTest {

    private final Base64Str transformer = new Base64Str();

    @Test
    public void encodesAndDecodesAsciiChineseAndEmojiWithUtf8() {
        assertEquals("base64_str", transformer.getTransformerName());
        for (String text : List.of("hello", "中文", "你好，世界 😀", "a\n\tb\r\n")) {
            String encoded = transform(text, "encode");
            assertEquals(Base64.getEncoder().encodeToString(text.getBytes(StandardCharsets.UTF_8)), encoded);
            assertEquals(text, transform(encoded, "decode"));
        }
        assertEquals("5Lit5paH", transform("中文", "encode"));
    }

    @Test
    public void optionalParametersMayBeOmittedNullEmptyOrWhitespace() {
        assertEquals("aGVsbG8=", transform("hello", "encode"));
        assertEquals("aGVsbG8=", transform("hello", "encode", null));
        assertEquals("aGVsbG8=", transform("hello", " encode ", " ", null));
        assertEquals("aGVsbG8=", transform("hello", "encode", "", "  "));
    }

    @Test
    public void supportsStandardAndUrlSafeAlphabetsWithPadding() {
        assertEquals("77+/", transform("\uffff", "encode", "standard"));
        assertEquals("77-_", transform("\uffff", "encode", "url_safe"));
        assertEquals("\uffff", transform("77-_", "decode", "url_safe"));
        assertEquals("YQ==", transform("a", "encode", "url_safe"));
        assertEquals("a", transform("YQ", "decode", "standard"));
    }

    @Test
    public void supportsExplicitCharsetAndAliases() {
        assertEquals("1tDOxA==", transform("中文", "encode", "", " GBK "));
        assertEquals("中文", transform("1tDOxA==", "decode", "standard", "GBK"));
        assertEquals("5Lit5paH", transform("中文", "encode", "standard", "utf8"));
    }

    @Test
    public void preservesNullWithoutChangingItsColumnType() {
        for (Column column : new Column[]{new StringColumn(), new LongColumn(), new BytesColumn()}) {
            DefaultRecord record = row(column);
            assertSame(record, transformer.evaluate(record, 1, "encode"));
            assertSame(column, record.getColumn(1));
            assertNull(record.getColumn(1).getRawData());
        }
        DefaultRecord record = row(null);
        assertSame(record, transformer.evaluate(record, 1, "decode"));
        assertNull(record.getColumn(1));
    }

    @Test
    public void convertsEmptyStringAndPreservesAllWhitespaceDuringEncoding() {
        assertEquals("", transform("", "encode"));
        assertEquals("", transform("", "decode"));
        assertEquals("IA==", transform(" ", "encode"));
        String text = " \t\r\n ";
        assertEquals(text, transform(transform(text, "encode"), "decode"));
    }

    @Test
    public void onlyReplacesTheRequestedColumnAndSupportsStringRepresentations() {
        DefaultRecord record = row(new LongColumn(123));
        Column first = record.getColumn(0);
        Column last = record.getColumn(2);
        assertSame(record, transformer.evaluate(record, 1, "encode"));
        assertEquals("MTIz", record.getColumn(1).asString());
        assertEquals(Column.Type.STRING, record.getColumn(1).getType());
        assertSame(first, record.getColumn(0));
        assertSame(last, record.getColumn(2));
        assertEquals(3, record.getColumnNumber());
    }

    @Test
    public void rejectsWrongParameterCountTypesAndOutOfBoundsColumn() {
        assertIllegal((Object[]) null);
        assertIllegal();
        assertIllegal(1);
        assertIllegal(1, "encode", "standard", "UTF-8", "extra");
        assertIllegal("1", "encode");
        assertIllegal(null, "encode");
        assertIllegal(-1, "encode");
        assertIllegal(3, "encode");
        assertIllegal(1, 1);
        assertIllegal(1, "encode", 1);
        assertIllegal(1, "encode", "standard", 1);
    }

    @Test
    public void rejectsMissingOrUnknownOperationVariantAndCharset() {
        assertIllegal(1, null);
        assertIllegal(1, "");
        assertIllegal(1, "  ");
        assertIllegal(1, "encrypt");
        assertIllegal(1, "ENCODE");
        assertIllegal(1, "encode", "mime");
        assertIllegal(1, "encode", "STANDARD");
        assertIllegal(1, "encode", "standard", "not-a-real-charset");
        assertIllegal(1, "encode", "standard", "UTF@8");
    }

    @Test
    public void validatesConfigurationEvenWhenFieldIsNull() {
        DefaultRecord record = row(new StringColumn());
        expectError(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
                () -> transformer.evaluate(record, 1, "wrong"));
        expectError(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
                () -> transformer.evaluate(null, 1, "encode"));
    }

    @Test
    public void rejectsMalformedBase64WhitespaceWrongPaddingAndWrongAlphabet() {
        for (String text : List.of("%%%", "A", "YQ=", "YQ===", "YQ==x", "Y Q==", "YQ==\n", "中文")) {
            assertRunFailure(text, "decode");
        }
        assertRunFailure("77-_", "decode", "standard");
        assertRunFailure("77+/", "decode", "url_safe");
        assertRunFailure(" ", "decode");
    }

    @Test
    public void rejectsMalformedOrUnmappableTextInsteadOfReplacingCharacters() {
        assertRunFailure("\ud800", "encode");
        assertRunFailure("中文", "encode", "standard", "US-ASCII");
        assertRunFailure("/w==", "decode");
        assertRunFailure("gA==", "decode", "standard", "US-ASCII");
    }

    @Test
    public void rejectsAllNonNullBinaryValuesWithoutMutatingTheRecord() {
        for (byte[] value : new byte[][]{new byte[0], "hello".getBytes(StandardCharsets.UTF_8), {(byte) 0xff}}) {
            for (String option : List.of("encode", "decode")) {
                DefaultRecord record = row(new BytesColumn(value));
                Column original = record.getColumn(1);
                expectError(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION,
                        () -> transformer.evaluate(record, 1, option));
                assertSame(original, record.getColumn(1));
                assertArrayEquals(value, record.getColumn(1).asBytes());
            }
        }
    }

    @Test
    public void sharedInstanceDoesNotLeakArgumentsBetweenThreads() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Callable<String>> calls = new ArrayList<>();
            for (int index = 0; index < 128; index++) {
                final String text = "并发 " + index;
                final String variant = index % 2 == 0 ? "standard" : "url_safe";
                calls.add(() -> transform(transform(text, "encode", variant), "decode", variant));
            }
            List<Future<String>> results = executor.invokeAll(calls);
            for (int index = 0; index < results.size(); index++) {
                assertEquals("并发 " + index, results.get(index).get());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private String transform(String value, Object... arguments) {
        Object[] paras = new Object[arguments.length + 1];
        paras[0] = 1;
        System.arraycopy(arguments, 0, paras, 1, arguments.length);
        return transformer.evaluate(row(new StringColumn(value)), paras).getColumn(1).asString();
    }

    private void assertIllegal(Object... arguments) {
        expectError(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
                () -> transformer.evaluate(row(new StringColumn("hello")), arguments));
    }

    private void assertRunFailure(String value, Object... arguments) {
        DefaultRecord record = row(new StringColumn(value));
        Column original = record.getColumn(1);
        Object[] paras = new Object[arguments.length + 1];
        paras[0] = 1;
        System.arraycopy(arguments, 0, paras, 1, arguments.length);
        expectError(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, () -> transformer.evaluate(record, paras));
        assertSame(original, record.getColumn(1));
        assertEquals(value, record.getColumn(1).asString());
    }

    private static void expectError(TransformerErrorCode code, Runnable action) {
        try {
            action.run();
            fail("expected " + code);
        } catch (AggregationException failure) {
            assertSame(code, failure.getErrorCode());
            assertTrue(failure.getMessage().contains("base64_str"));
        }
    }

    private static DefaultRecord row(Column column) {
        DefaultRecord record = new DefaultRecord();
        record.addColumn(new StringColumn("before"));
        record.addColumn(column);
        record.addColumn(new StringColumn("after"));
        return record;
    }
}
