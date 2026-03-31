package com.jdragon.aggregation.transformer.impl;

import java.util.HashMap;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public class DESEDEEncryption {
    public static final String KEY_ALGORITHM = "DESede";
    private static final int KEY_SIZE = 168;

    public DESEDEEncryption() {
    }

    public Map<String, Object> generateKey() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("DESede");
        keyGenerator.init(168);
        SecretKey publicKey = keyGenerator.generateKey();
        Map<String, Object> keyMap = new HashMap<>();
        keyMap.put("PublicKey", publicKey);
        return keyMap;
    }

    public byte[] encrypt(byte[] key, byte[] data) throws Exception {
        SecretKey secretKey = new SecretKeySpec(key, "DESede");
        Cipher cipher = Cipher.getInstance("DESede");
        cipher.init(1, secretKey);
        return cipher.doFinal(data);
    }

    public byte[] decrypt(byte[] key, byte[] data) throws Exception {
        SecretKey secretKey = new SecretKeySpec(key, "DESede");
        Cipher cipher = Cipher.getInstance("DESede");
        cipher.init(2, secretKey);
        return cipher.doFinal(data);
    }
}
