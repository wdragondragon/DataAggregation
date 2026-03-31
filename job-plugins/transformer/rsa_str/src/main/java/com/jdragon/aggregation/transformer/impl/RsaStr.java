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
 * RSA加密
 *
 * @Author jdragon
 * @Date 2026/3/27
 */
public class RsaStr extends Transformer {

    static RSAEncryption rsaEncryption = new RSAEncryption();

    public RsaStr() {
        super.setTransformerName("rsa_str");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        int columnIndex;
        String key;
        String option;
        try {
            if (paras.length != 3) {
                throw new RuntimeException("rsa_str paras must be 3");
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
    private static String encrypt(String oriValue, String publicKey) throws Exception {
        //公钥加密
        return Base64.encode(rsaEncryption.encrypt(Base64.decode(publicKey), oriValue.getBytes()));
    }

    //解密操作
    private static String decrypt(String oriValue, String privateKey) throws Exception {
        return new String(rsaEncryption.decrypt(Base64.decode(privateKey), Base64.decode(oriValue)));
    }


    public static void main(String[] args) throws Exception {
        //对方生成的公私密钥对。64整数位
        //生成key http://web.chacuo.net/netrsakeypair
        String publicKey = "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAO5gaoDJqqTq3CDnq5YBg1BJ2QYArlggYfftbGcLXaaxK1j9clAcpCXKROoivKgIvjy/mumKBCbtpPWqtI9pSq8CAwEAAQ==";
        String privateKey = "MIIBVQIBADANBgkqhkiG9w0BAQEFAASCAT8wggE7AgEAAkEA7mBqgMmqpOrcIOerlgGDUEnZBgCuWCBh9+1sZwtdprErWP1yUBykJcpE6iK8qAi+PL+a6YoEJu2k9aq0j2lKrwIDAQABAkEAkIf1C1E7HfMotOrCppkUPUIJTBJtoxE/VUunRnMlvULTLcN3wIcXdmLU3rxtX3QC62QPdfqJ0l6261cAJUASQQIhAPc7A0svG5JvNXHpEB0DT0jC51jRYchxaUnxpBvKQrnNAiEA9tUBSAmPk+t/kZbxdPF1dk3waYJri8VWHZ4/Ue0yKmsCIBdPPhFBoMTerVhPFBDYNgpzLeLG4wRGBRpOqR1hpYblAiEAi8o+m4muottwuAeAX/aPy5yAV4DhX5s3FjcVLVTkYFkCICklRrL5R9d/NNAlQJMxYk7aYZxLI6dKUwilV0+PqL/W";
        String s = encrypt("1", publicKey);
        System.out.println(s);
        s = decrypt(s, privateKey);
        System.out.println(s);
    }
}
