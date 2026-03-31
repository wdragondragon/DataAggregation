package com.jdragon.aggregation.transformer.impl;

import java.security.Key;
import java.util.Base64;
import java.util.Map;

public interface EncryptionStrategy {
    Base64.Encoder base64Encoder = Base64.getEncoder();
    Base64.Decoder base64Decoder = Base64.getDecoder();
    String PUBLIC_KEY = "PublicKey";
    String PRIVATE_KEY = "PrivateKey";

    Map<String, Object> generateKey() throws Exception;

    byte[] encrypt(byte[] key, byte[] data) throws Exception;

    byte[] decrypt(byte[] key, byte[] data) throws Exception;

    default byte[] getPublicKey(Map<String, Object> keyMap) {
        Key key = (Key)keyMap.get("PublicKey");
        return key.getEncoded();
    }

    default byte[] getPrivateKey(Map<String, Object> keyMap) {
        Key key = (Key)keyMap.get("PrivateKey");
        return key.getEncoded();
    }

    default String getPublicKeyString(Map<String, Object> keyMap) {
        Key key = (Key)keyMap.get("PublicKey");
        byte[] encoded = key.getEncoded();
        return base64Encoder.encodeToString(encoded);
    }

    default String getPrivateKeyString(Map<String, Object> keyMap) {
        Key key = (Key)keyMap.get("PrivateKey");
        byte[] encoded = key.getEncoded();
        return base64Encoder.encodeToString(encoded);
    }

    default byte[] publicKeyStringToByte(String publicKey) {
        return base64Decoder.decode(publicKey);
    }

    default byte[] privateKeyStringToByte(String privateKey) {
        return base64Decoder.decode(privateKey);
    }

    default String bytesToBase64String(byte[] data) {
        return base64Encoder.encodeToString(data);
    }

    default byte[] base64StringToBytes(String base64String) {
        return base64Decoder.decode(base64String);
    }
}
