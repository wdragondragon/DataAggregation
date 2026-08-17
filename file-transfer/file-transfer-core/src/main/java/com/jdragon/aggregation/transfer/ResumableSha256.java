package com.jdragon.aggregation.transfer;

import org.bouncycastle.crypto.digests.SHA256Digest;

import java.io.IOException;
import java.util.Base64;
import java.util.HexFormat;

final class ResumableSha256 {

    private final SHA256Digest digest;

    private ResumableSha256(SHA256Digest digest) {
        this.digest = digest;
    }

    static ResumableSha256 create() {
        return new ResumableSha256(new SHA256Digest());
    }

    static ResumableSha256 restore(String encodedState) throws IOException {
        try {
            return new ResumableSha256(new SHA256Digest(Base64.getDecoder().decode(encodedState)));
        } catch (RuntimeException exception) {
            throw new IOException("checkpoint SHA-256 state is invalid", exception);
        }
    }

    void update(byte[] input, int offset, int length) {
        digest.update(input, offset, length);
    }

    String encodedState() {
        return Base64.getEncoder().encodeToString(digest.getEncodedState());
    }

    String digestHex() {
        SHA256Digest snapshot = new SHA256Digest(digest);
        byte[] value = new byte[snapshot.getDigestSize()];
        snapshot.doFinal(value, 0);
        return HexFormat.of().formatHex(value);
    }
}
