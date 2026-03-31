package com.jdragon.aggregation.transformer.impl;

import cn.hutool.core.codec.Base64;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * SM2加密
 *
 * @Author jdragon
 * @Date 2026/3/27
 */
public class Sm2Str extends Transformer {

    static SM2Encryption sm2Encryption = new SM2Encryption();

    public Sm2Str() {
        super.setTransformerName("sm2_str");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        int columnIndex;
        String key;
        String option;
        try {
            if (paras.length != 3) {
                throw new RuntimeException("sm2_str paras must be 3");
            }
            columnIndex = (Integer) paras[0];
            key = paras[1].toString();
            option = paras[2].toString();
        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras) + " => " + e.getMessage());
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
    private static String encrypt(String oriValue, String publicKey) throws Exception {
        //公钥加密
        return Base64.encode(sm2Encryption.encrypt(Base64.decode(publicKey), oriValue.getBytes()));
    }

    //解密操作
    private static String decrypt(String oriValue, String privateKey) throws Exception {
        return new String(sm2Encryption.decrypt(Base64.decode(privateKey), Base64.decode(oriValue)));
    }


    public static void main(String[] args) throws Exception {
        String publicKey = "BBgTjMbYodFgkiA2LRJSeA5fz068WRqo5M7xUMJZ8r0t+3EEZfnyv45p9pYUz2Nu0pINkeev5SvWEo1E2AZhx5k=";
        String privateKey = "AN8yg1c/wjyPt3bvD8OXoaQnfxTpo+8GjRl7ZDQ8Axda";
        String oriValue = encrypt("123", publicKey);
        System.out.println(oriValue);
        oriValue = decrypt(oriValue, privateKey);
        System.out.println(oriValue);
    }
}
