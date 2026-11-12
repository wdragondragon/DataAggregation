package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.core.transformer.TransformerErrorCode;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.util.Base64;

/** Encodes and decodes a text field using the selected Base64 alphabet and charset. */
public final class Base64Str extends Transformer {

    public Base64Str() {
        setTransformerName("base64_str");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        if (record == null || paras == null || paras.length < 2 || paras.length > 4
                || !(paras[0] instanceof Integer)) {
            throw illegalParameter("expected columnIndex, option[, variant[, charset]]");
        }
        int columnIndex = (Integer) paras[0];
        if (columnIndex < 0 || columnIndex >= record.getColumnNumber()) {
            throw illegalParameter("columnIndex is out of bounds");
        }
        String option = stringParameter(paras[1], "option", null);
        String variant = stringParameter(paras.length > 2 ? paras[2] : null, "variant", "standard");
        String charsetName = stringParameter(paras.length > 3 ? paras[3] : null, "charset", "UTF-8");
        if (!"encode".equals(option) && !"decode".equals(option)) {
            throw illegalParameter("option must be encode or decode");
        }
        if (!"standard".equals(variant) && !"url_safe".equals(variant)) {
            throw illegalParameter("variant must be standard or url_safe");
        }
        Charset charset;
        try {
            charset = Charset.forName(charsetName);
        } catch (IllegalArgumentException failure) {
            throw illegalParameter("charset is not supported");
        }

        Column column = record.getColumn(columnIndex);
        if (column == null || column.getRawData() == null) {
            return record;
        }
        try {
            if (column.getType() == Column.Type.BYTES) {
                throw new IllegalArgumentException("base64_str only supports text fields");
            }
            String value = column.asString();
            String converted;
            if ("encode".equals(option)) {
                ByteBuffer encoded = charset.newEncoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .encode(CharBuffer.wrap(value));
                byte[] bytes = new byte[encoded.remaining()];
                encoded.get(bytes);
                Base64.Encoder encoder = "url_safe".equals(variant)
                        ? Base64.getUrlEncoder() : Base64.getEncoder();
                converted = encoder.encodeToString(bytes);
            } else {
                Base64.Decoder decoder = "url_safe".equals(variant)
                        ? Base64.getUrlDecoder() : Base64.getDecoder();
                byte[] bytes = decoder.decode(value);
                converted = charset.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(bytes)).toString();
            }
            record.setColumn(columnIndex, new StringColumn(converted));
            return record;
        } catch (Exception failure) {
            AggregationException exception = AggregationException.asException(
                    TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION,
                    "base64_str " + option + " failed at column " + columnIndex
                            + "; expected text compatible with the selected Base64 variant and charset");
            exception.initCause(failure);
            throw exception;
        }
    }

    private static String stringParameter(Object value, String name, String defaultValue) {
        if (value != null && !(value instanceof String)) {
            throw illegalParameter(name + " must be a string");
        }
        String text = value == null ? "" : ((String) value).trim();
        if (text.isEmpty()) {
            if (defaultValue == null) {
                throw illegalParameter(name + " is required");
            }
            return defaultValue;
        }
        return text;
    }

    private static AggregationException illegalParameter(String message) {
        return AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER,
                "base64_str " + message);
    }
}
