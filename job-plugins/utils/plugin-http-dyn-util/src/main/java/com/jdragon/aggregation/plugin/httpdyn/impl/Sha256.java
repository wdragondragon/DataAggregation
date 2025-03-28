package com.jdragon.aggregation.plugin.httpdyn.impl;


import com.jdragon.aggregation.plugin.httpdyn.BaseHttpDynExecutor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author tfq
 */
@Slf4j
public class Sha256 extends BaseHttpDynExecutor {
    public Sha256() {
        super.setHttpDynName("dyn_Sha256");
    }

    @Override
    public String execute(Object... paras) {
        String oriValue = "";
        if (isParamIsNotBank(paras)) {
            if (paras[0] != null) {
                String param0 = paras[0].toString();
                if (StringUtils.isNotBlank(param0)) {
                    oriValue = getSHA256StrJava(param0);
                }
            }
        }
        return oriValue;
    }

    /**
     * 利用java原生的摘要实现SHA256加密
     *
     * @param str 加密后的报文
     */
    public static String getSHA256StrJava(String str) {
        MessageDigest messageDigest;
        String encodeStr = "";
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
            messageDigest.update(str.getBytes(StandardCharsets.UTF_8));
            encodeStr = byte2Hex(messageDigest.digest());
        } catch (NoSuchAlgorithmException e) {
            log.error(e.getMessage(), e);
        }
        return encodeStr;
    }

    /**
     * 将byte转为16进制
     */
    private static String byte2Hex(byte[] bytes) {
        StringBuilder stringBuffer = new StringBuilder();
        String temp;
        for (int i = 0; i < bytes.length; i++) {
            temp = Integer.toHexString(bytes[i] & 0xFF);
            if (temp.length() == 1) {
                //1得到一位的进行补0操作
                stringBuffer.append("0");
            }
            stringBuffer.append(temp);
        }
        return stringBuffer.toString();
    }
}
