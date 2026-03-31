package com.jdragon.aggregation.transformer.impl;


import java.security.Security;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

public class IDEAEncryption {
    public static final String KEY_ALGORITHM = "IDEA";
    private static final int KEY_SIZE = 128;

    public IDEAEncryption() {
    }

    public Map<String, Object> generateKey() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("IDEA");
        keyGenerator.init(128);
        SecretKey publicKey = keyGenerator.generateKey();
        Map<String, Object> keyMap = new HashMap();
        keyMap.put("PublicKey", publicKey);
        return keyMap;
    }

    public byte[] encrypt(byte[] key, byte[] data) throws Exception {
        SecretKey secretKey = new SecretKeySpec(key, "IDEA");
        Cipher cipher = Cipher.getInstance("IDEA");
        cipher.init(1, secretKey);
        return cipher.doFinal(data);
    }

    public byte[] decrypt(byte[] key, byte[] data) throws Exception {
        SecretKey secretKey = new SecretKeySpec(key, "IDEA");
        Cipher cipher = Cipher.getInstance("IDEA");
        cipher.init(2, secretKey);
        return cipher.doFinal(data);
    }

    static {
        Security.addProvider(new BouncyCastleProvider());
    }
}
