package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.transfer.model.TransferPolicy;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class TransferSpecNormalizerTest {

    @Test
    public void shouldApplyCanonicalPolicyDefaults() {
        Map<String, Object> policy = TransferSpecNormalizer.normalizePolicyMap(Map.of());

        assertEquals("FAIL", policy.get("conflictPolicy"));
        assertEquals("SHA-256", policy.get("checksumAlgorithm"));
        assertEquals("STRONG", policy.get("verificationMode"));
        assertEquals(TransferPolicy.DEFAULT_VERIFICATION_FRAME_COUNT,
                policy.get("verificationFrameCount"));
    }

    @Test
    public void shouldExposeInvalidField() {
        TransferContractException exception = assertThrows(TransferContractException.class,
                () -> TransferSpecNormalizer.normalizePolicyMap(Map.of(
                        "verificationFrameCount", 0)));

        assertEquals("policy", exception.field());
        assertEquals("INVALID_TRANSFER_SPEC", exception.code());
    }
}
