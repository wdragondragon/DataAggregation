package com.jdragon.aggregation.transformer.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.bouncycastle.crypto.params.ECDomainParameters;
import org.bouncycastle.math.ec.ECCurve;
import org.bouncycastle.math.ec.ECPoint;

public class SM2Encryption implements EncryptionStrategy {
    private static final BigInteger n = new BigInteger("FFFFFFFEFFFFFFFFFFFFFFFFFFFFFFFF7203DF6B21C6052B53BBF40939D54123", 16);
    private static final BigInteger p = new BigInteger("FFFFFFFEFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00000000FFFFFFFFFFFFFFFF", 16);
    private static final BigInteger a = new BigInteger("FFFFFFFEFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00000000FFFFFFFFFFFFFFFC", 16);
    private static final BigInteger b = new BigInteger("28E9FA9E9D9F5E344D5A9E4BCF6509A7F39789F515AB8F92DDBCBD414D940E93", 16);
    private static final BigInteger gx = new BigInteger("32C4AE2C1F1981195F9904466A39C9948FE30BBFF2660BE1715A4589334C74C7", 16);
    private static final BigInteger gy = new BigInteger("BC3736A2F4F6779C59BDCEE36B692153D0A9877CC62A474002DF32E52139F0A0", 16);
    private static ECDomainParameters ecc_bc_spec;
    private static final int DIGEST_LENGTH = 32;
    private static final SecureRandom random = new SecureRandom();
    private static ECCurve.Fp curve;
    private static ECPoint G;
    private static final char[] hexDigits = new char[]{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    private static final String ivHexStr = "7380166f 4914b2b9 172442d7 da8a0600 a96f30bc 163138aa e38dee4d b0fb0e4e";
    private static final BigInteger IV = new BigInteger("7380166f 4914b2b9 172442d7 da8a0600 a96f30bc 163138aa e38dee4d b0fb0e4e".replaceAll(" ", ""), 16);
    private static final Integer Tj15 = Integer.valueOf("79cc4519", 16);
    private static final Integer Tj63 = Integer.valueOf("7a879d8a", 16);
    private static final byte[] FirstPadding = new byte[]{-128};
    private static final byte[] ZeroPadding = new byte[]{0};

    public SM2Encryption() {
        curve = new ECCurve.Fp(p, a, b);
        G = curve.createPoint(gx, gy);
        ecc_bc_spec = new ECDomainParameters(curve, G, n);
    }

    public Map<String, Object> generateKey() throws Exception {
        BigInteger privateKey = random(n.subtract(new BigInteger("1")));
        ECPoint publicKey = G.multiply(privateKey).normalize();
        Map<String, Object> keyMap = new HashMap();
        if (!this.checkPublicKey(publicKey)) {
            throw new Exception("生成的公钥不合法！");
        } else {
            keyMap.put("PublicKey", publicKey);
            keyMap.put("PrivateKey", privateKey);
            return keyMap;
        }
    }

    public byte[] getPublicKey(Map<String, Object> keyMap) {
        ECPoint publicKey = (ECPoint)keyMap.get("PublicKey");
        return publicKey.getEncoded(false);
    }

    public byte[] getPrivateKey(Map<String, Object> keyMap) {
        BigInteger privateKey = (BigInteger)keyMap.get("PrivateKey");
        return privateKey.toByteArray();
    }

    public String getPublicKeyString(Map<String, Object> keyMap) {
        ECPoint publicKey = (ECPoint)keyMap.get("PublicKey");
        byte[] encoded = publicKey.getEncoded(false);
        return base64Encoder.encodeToString(encoded);
    }

    public String getPrivateKeyString(Map<String, Object> keyMap) {
        BigInteger privateKey = (BigInteger)keyMap.get("PrivateKey");
        return base64Encoder.encodeToString(privateKey.toByteArray());
    }

    private static BigInteger random(BigInteger max) {
        BigInteger r;
        for(r = new BigInteger(256, random); r.compareTo(max) >= 0; r = new BigInteger(128, random)) {
        }

        return r;
    }

    private boolean checkPublicKey(ECPoint publicKey) {
        if (!publicKey.isInfinity()) {
            BigInteger x = publicKey.getXCoord().toBigInteger();
            BigInteger y = publicKey.getYCoord().toBigInteger();
            if (this.between(x, new BigInteger("0"), p) && this.between(y, new BigInteger("0"), p)) {
                BigInteger xResult = x.pow(3).add(a.multiply(x)).add(b).mod(p);
                BigInteger yResult = y.pow(2).mod(p);
                return yResult.equals(xResult) && publicKey.multiply(n).isInfinity();
            }
        }

        return false;
    }

    private boolean between(BigInteger param, BigInteger min, BigInteger max) {
        return param.compareTo(min) >= 0 && param.compareTo(max) < 0;
    }

    public byte[] encrypt(byte[] key, byte[] data) throws Exception {
        ECPoint publicKey = curve.decodePoint(key);

        byte[] C1Buffer;
        ECPoint kpb;
        byte[] t;
        do {
            BigInteger k = random(n);
            ECPoint C1 = G.multiply(k);
            C1Buffer = C1.getEncoded(false);
            BigInteger h = ecc_bc_spec.getH();
            if (h != null) {
                ECPoint S = publicKey.multiply(h);
                if (S.isInfinity()) {
                    throw new IllegalStateException();
                }
            }

            kpb = publicKey.multiply(k).normalize();
            byte[] kpbBytes = kpb.getEncoded(false);
            t = KDF(kpbBytes, data.length);
        } while(this.allZero(t));

        byte[] C2 = new byte[data.length];

        for(int i = 0; i < data.length; ++i) {
            C2[i] = (byte)(data[i] ^ t[i]);
        }

        byte[] C3 = sm3hash(kpb.getXCoord().toBigInteger().toByteArray(), data, kpb.getYCoord().toBigInteger().toByteArray());
        byte[] encryptResult = new byte[C1Buffer.length + C2.length + C3.length];
        System.arraycopy(C1Buffer, 0, encryptResult, 0, C1Buffer.length);
        System.arraycopy(C2, 0, encryptResult, C1Buffer.length, C2.length);
        System.arraycopy(C3, 0, encryptResult, C1Buffer.length + C2.length, C3.length);
        return encryptResult;
    }

    private boolean allZero(byte[] buffer) {
        for(int i = 0; i < buffer.length; ++i) {
            if (buffer[i] != 0) {
                return false;
            }
        }

        return true;
    }

    private static byte[] KDF(byte[] Z, int klen) {
        int ct = 1;
        int end = (int)Math.ceil((double) klen / 32.0);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try {
            for(int i = 1; i < end; ++i) {
                baos.write(sm3hash(Z, toByteArray(ct)));
                ++ct;
            }

            byte[] last = sm3hash(Z, toByteArray(ct));
            if (klen % 32 == 0) {
                baos.write(last);
            } else {
                baos.write(last, 0, klen % 32);
            }

            return baos.toByteArray();
        } catch (Exception var6) {
            Exception e = var6;
            e.printStackTrace();
            return null;
        }
    }

    public static byte[] toByteArray(int i) {
        byte[] byteArray = new byte[]{(byte)(i >>> 24), (byte)((i & 16777215) >>> 16), (byte)((i & '\uffff') >>> 8), (byte)(i & 255)};
        return byteArray;
    }

    private static byte[] sm3hash(byte[]... params) {
        byte[] res = null;

        try {
            res = hash(join(params));
        } catch (IOException var3) {
            IOException e = var3;
            e.printStackTrace();
        }

        return res;
    }

    public static byte[] hash(byte[] source) throws IOException {
        byte[] m1 = padding(source);
        int n = m1.length / 64;
        byte[] vi = IV.toByteArray();
        byte[] vi1 = null;

        for(int i = 0; i < n; ++i) {
            byte[] b = Arrays.copyOfRange(m1, i * 64, (i + 1) * 64);
            vi1 = CF(vi, b);
            vi = vi1;
        }

        return vi1;
    }

    private static byte[] padding(byte[] source) throws IOException {
        if ((long)source.length >= 2305843009213693952L) {
            throw new RuntimeException("src data invalid.");
        } else {
            long l = source.length * 8L;
            long k = 448L - (l + 1L) % 512L;
            if (k < 0L) {
                k += 512L;
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            baos.write(source);
            baos.write(FirstPadding);

            for(long i = k - 7L; i > 0L; i -= 8L) {
                baos.write(ZeroPadding);
            }

            baos.write(long2bytes(l));
            return baos.toByteArray();
        }
    }

    private static byte[] CF(byte[] vi, byte[] bi) throws IOException {
        int a = toInteger(vi, 0);
        int b = toInteger(vi, 1);
        int c = toInteger(vi, 2);
        int d = toInteger(vi, 3);
        int e = toInteger(vi, 4);
        int f = toInteger(vi, 5);
        int g = toInteger(vi, 6);
        int h = toInteger(vi, 7);
        int[] w = new int[68];
        int[] w1 = new int[64];

        int ss1;
        for(ss1 = 0; ss1 < 16; ++ss1) {
            w[ss1] = toInteger(bi, ss1);
        }

        for(ss1 = 16; ss1 < 68; ++ss1) {
            w[ss1] = P1(w[ss1 - 16] ^ w[ss1 - 9] ^ Integer.rotateLeft(w[ss1 - 3], 15)) ^ Integer.rotateLeft(w[ss1 - 13], 7) ^ w[ss1 - 6];
        }

        for(ss1 = 0; ss1 < 64; ++ss1) {
            w1[ss1] = w[ss1] ^ w[ss1 + 4];
        }

        for(int j = 0; j < 64; ++j) {
            ss1 = Integer.rotateLeft(Integer.rotateLeft(a, 12) + e + Integer.rotateLeft(T(j), j), 7);
            int ss2 = ss1 ^ Integer.rotateLeft(a, 12);
            int tt1 = FF(a, b, c, j) + d + ss2 + w1[j];
            int tt2 = GG(e, f, g, j) + h + ss1 + w[j];
            d = c;
            c = Integer.rotateLeft(b, 9);
            b = a;
            a = tt1;
            h = g;
            g = Integer.rotateLeft(f, 19);
            f = e;
            e = P0(tt2);
        }

        byte[] v = toByteArray(a, b, c, d, e, f, g, h);

        for(int i = 0; i < v.length; ++i) {
            v[i] ^= vi[i];
        }

        return v;
    }

    private static byte[] join(byte[]... params) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] res = null;

        try {
            for(int i = 0; i < params.length; ++i) {
                baos.write(params[i]);
            }

            res = baos.toByteArray();
        } catch (IOException var4) {
            IOException e = var4;
            e.printStackTrace();
        }

        return res;
    }

    private static int toInteger(byte[] source, int index) {
        StringBuilder valueStr = new StringBuilder();

        for(int i = 0; i < 4; ++i) {
            valueStr.append(hexDigits[(byte)((source[index * 4 + i] & 240) >> 4)]);
            valueStr.append(hexDigits[(byte)(source[index * 4 + i] & 15)]);
        }

        return Long.valueOf(valueStr.toString(), 16).intValue();
    }

    private static Integer FF(Integer x, Integer y, Integer z, int j) {
        if (j >= 0 && j <= 15) {
            return x ^ y ^ z;
        } else if (j >= 16 && j <= 63) {
            return x & y | x & z | y & z;
        } else {
            throw new RuntimeException("data invalid");
        }
    }

    private static Integer GG(Integer x, Integer y, Integer z, int j) {
        if (j >= 0 && j <= 15) {
            return x ^ y ^ z;
        } else if (j >= 16 && j <= 63) {
            return x & y | ~x & z;
        } else {
            throw new RuntimeException("data invalid");
        }
    }

    private static Integer P0(Integer x) {
        return x ^ Integer.rotateLeft(x, 9) ^ Integer.rotateLeft(x, 17);
    }

    private static Integer P1(Integer x) {
        return x ^ Integer.rotateLeft(x, 15) ^ Integer.rotateLeft(x, 23);
    }

    private static byte[] toByteArray(int a, int b, int c, int d, int e, int f, int g, int h) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(32);
        baos.write(toByteArray(a));
        baos.write(toByteArray(b));
        baos.write(toByteArray(c));
        baos.write(toByteArray(d));
        baos.write(toByteArray(e));
        baos.write(toByteArray(f));
        baos.write(toByteArray(g));
        baos.write(toByteArray(h));
        return baos.toByteArray();
    }

    private static int T(int j) {
        if (j >= 0 && j <= 15) {
            return Tj15;
        } else if (j >= 16 && j <= 63) {
            return Tj63;
        } else {
            throw new RuntimeException("data invalid");
        }
    }

    private static byte[] long2bytes(long l) {
        byte[] bytes = new byte[8];

        for(int i = 0; i < 8; ++i) {
            bytes[i] = (byte)((int)(l >>> (7 - i) * 8));
        }

        return bytes;
    }

    public byte[] decrypt(byte[] key, byte[] data) throws Exception {
        BigInteger privateKey = new BigInteger(key);
        byte[] C1Byte = new byte[65];
        System.arraycopy(data, 0, C1Byte, 0, C1Byte.length);
        ECPoint C1 = curve.decodePoint(C1Byte).normalize();
        BigInteger h = ecc_bc_spec.getH();
        ECPoint dBC1;
        if (h != null) {
            dBC1 = C1.multiply(h);
            if (dBC1.isInfinity()) {
                throw new IllegalStateException();
            }
        }

        dBC1 = C1.multiply(privateKey).normalize();
        byte[] dBC1Bytes = dBC1.getEncoded(false);
        int klen = data.length - 65 - 32;
        byte[] t = KDF(dBC1Bytes, klen);
        if (this.allZero(t)) {
            System.err.println("all zero");
            throw new IllegalStateException();
        } else {
            byte[] M = new byte[klen];

            for(int i = 0; i < M.length; ++i) {
                M[i] = (byte)(data[C1Byte.length + i] ^ t[i]);
            }

            byte[] C3 = new byte[32];
            System.arraycopy(data, data.length - 32, C3, 0, 32);
            byte[] u = sm3hash(dBC1.getXCoord().toBigInteger().toByteArray(), M, dBC1.getYCoord().toBigInteger().toByteArray());
            return Arrays.equals(u, C3) ? M : null;
        }
    }
}
