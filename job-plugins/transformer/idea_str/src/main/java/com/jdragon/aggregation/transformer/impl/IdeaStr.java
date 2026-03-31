package com.jdragon.aggregation.transformer.impl;

import cn.hutool.core.codec.Base64;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * @Author jdragon
 * @Date 2026/3/27
 */
public class IdeaStr extends Transformer {

    IDEAEncryption ideaEncryption = new IDEAEncryption();

    public IdeaStr() {
        super.setTransformerName("bm_ideaNew_str");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        int columnIndex;
        String key;
        String option;
        try {
            if (paras.length != 3) {
                throw new RuntimeException("idea_str paras must be 3");
            }
            columnIndex = (Integer) paras[0];
            key = paras[1].toString();
            option = paras[2].toString();
        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);
        try {
            String oriValue = column.asString();
            if (StringUtils.isBlank(oriValue)) {
                return record;
            } else {
                //加密操作
                if ("encrypt".equals(option)) {
                    oriValue = encrypt(oriValue, key);
                }//解密操作
                else if ("decrypt".equals(option)) {
                    oriValue = decrypt(oriValue, key);
                }
                record.setColumn(columnIndex, new StringColumn(oriValue));
            }
        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }

    //加密操作
    @SneakyThrows
    public static String encrypt(String content, String key) {
        IDEAEncryption ideaEncryption = new IDEAEncryption();
        return Base64.encode(ideaEncryption.encrypt(Base64.decode(key), content.getBytes()));
    }

    //解密操作
    @SneakyThrows
    public static String decrypt(String content, String key) {
        IDEAEncryption ideaEncryption = new IDEAEncryption();
        byte[] bytes = ideaEncryption.decrypt(Base64.decode(key), Base64.decode(content));
        return new String(bytes);
    }

    public static void main(String[] args) {
        //key 16位
        String key = Base64.encode("1234567890123456".getBytes(StandardCharsets.UTF_8));
        String content = "1";
        String encrypt = encrypt(content, key);
        System.out.println(encrypt);

        String decrypt = decrypt(encrypt, key);
        System.out.println(decrypt);

    }
}
