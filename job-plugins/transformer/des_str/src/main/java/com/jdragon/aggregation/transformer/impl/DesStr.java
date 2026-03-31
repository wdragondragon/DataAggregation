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
 * DES加密
 *
 * @Author jdragon
 * @Date 2026/3/27
 * 8位 key
 */
public class DesStr extends Transformer {

    DESEncryption desEncryption = new DESEncryption();

    public DesStr() {
        super.setTransformerName("des_str");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        int columnIndex;
        String key;
        String option;
        try {
            if (paras.length != 3) {
                throw new RuntimeException("des_str paras must be 3");
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
                    oriValue = Base64.encode(desEncryption.encrypt(Base64.decode(key), oriValue.getBytes()));
                }//解密操作
                else if ("decrypt".equals(option)) {
                    byte[] bytes = desEncryption.decrypt(Base64.decode(key), Base64.decode(oriValue));
                    oriValue = new String(bytes);
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
        DESEncryption desEncryption = new DESEncryption();
        return Base64.encode(desEncryption.encrypt(Base64.decode(key), content.getBytes()));
    }

    //解密操作
    @SneakyThrows
    public static String decrypt(String content, String key) {
        DESEncryption desEncryption = new DESEncryption();
        byte[] bytes = desEncryption.decrypt(Base64.decode(key), Base64.decode(content));
        return new String(bytes);
    }

    public static void main(String[] args) {
        //key 必须8位
        String key = "12345678";
        key = Base64.encode(key.getBytes(StandardCharsets.UTF_8));
        String encrypt = encrypt("1", key);
        System.out.println(encrypt);
        System.out.println(decrypt(encrypt, key));
    }
}
