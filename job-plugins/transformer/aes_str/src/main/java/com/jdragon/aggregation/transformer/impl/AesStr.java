package com.jdragon.aggregation.transformer.impl;

import cn.hutool.core.codec.Base64;
import cn.hutool.crypto.SecureUtil;
import cn.hutool.crypto.symmetric.AES;
import cn.hutool.crypto.symmetric.SymmetricAlgorithm;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * <p>
 * <p>AES加密
 * </p>
 *
 * @author jdragon
 * @since 2026/3/27
 */
public class AesStr extends Transformer {

    public AesStr() {
        super.setTransformerName("aes_str");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        int columnIndex;
        String key;
        String option;
        try {
            if (paras.length != 3) {
                throw new RuntimeException("aes_str paras must be 3");
            }
            columnIndex = (Integer) paras[0];
            key = (String) paras[1];
            option = (String) paras[2];
        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);
        try {
            String oriValue = column.asString();
            if (StringUtils.isBlank(oriValue)) {
                return record;
            } else {
                byte[] keyBytes = Base64.decode(key);
                //加密操作
                if ("encrypt".equals(option)) {
                    oriValue = encrypt(oriValue, keyBytes);
                }//解密操作
                else if ("decrypt".equals(option)) {
                    oriValue = decrypt(oriValue, keyBytes);
                }
                record.setColumn(columnIndex, new StringColumn(oriValue));
            }
        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }

    //加密操作
    public static String encrypt(String content, byte[] keyBytes) {
        byte[] keyByte = SecureUtil.generateKey(SymmetricAlgorithm.AES.getValue(), keyBytes).getEncoded();
        AES aes = SecureUtil.aes(keyByte);
        byte[] encrypt = aes.encrypt(content);
        return Base64.encode(encrypt);
    }

    //解密操作
    public static String decrypt(String content, byte[] keyBytes) {
        byte[] keyByte = SecureUtil.generateKey(SymmetricAlgorithm.AES.getValue(), keyBytes).getEncoded();
        AES aes = SecureUtil.aes(keyByte);
        byte[] decrypt = aes.decrypt(Base64.decode(content));
        return new String(decrypt);
    }

    public static void main(String[] args) {
        //key 必须16位
        byte[] keyBytes = "1234567890123456".getBytes(StandardCharsets.UTF_8);
        String content = "1";
        String encrypt = encrypt(content, keyBytes);
        System.out.println(encrypt);
        String decrypt = decrypt(encrypt, keyBytes);
        System.out.println(decrypt);
    }
}
