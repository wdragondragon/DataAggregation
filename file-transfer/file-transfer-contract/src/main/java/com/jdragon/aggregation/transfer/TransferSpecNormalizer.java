package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.transfer.model.ConflictPolicy;
import com.jdragon.aggregation.transfer.model.SourceSuccessAction;
import com.jdragon.aggregation.transfer.model.TransferPolicy;
import com.jdragon.aggregation.transfer.model.TransferSpec;
import com.jdragon.aggregation.transfer.model.VerificationMode;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

public final class TransferSpecNormalizer {

    private TransferSpecNormalizer() {
    }

    public static TransferSpec normalize(TransferSpec spec) {
        if (spec == null) {
            throw invalid("spec", "file transfer specification is required", null);
        }
        try {
            return new TransferSpec(spec.schemaVersion(), spec.source(), spec.target(),
                    spec.selection(), spec.mapping(), spec.policy(), spec.runtime(),
                    spec.timeZone(), spec.parameters());
        } catch (IllegalArgumentException exception) {
            throw invalid("spec", exception.getMessage(), exception);
        }
    }

    public static TransferPolicy normalizePolicy(Map<String, ?> values) {
        Map<String, ?> source = values == null ? Map.of() : values;
        try {
            return new TransferPolicy(
                    enumValue(ConflictPolicy.class, source.get("conflictPolicy"), ConflictPolicy.FAIL,
                            "policy.conflictPolicy"),
                    stringValue(source.get("checksumAlgorithm"), "SHA-256").toUpperCase(Locale.ROOT),
                    enumValue(VerificationMode.class, source.get("verificationMode"),
                            VerificationMode.STRONG, "policy.verificationMode"),
                    integerValue(source.get("verificationFrameCount"),
                            TransferPolicy.DEFAULT_VERIFICATION_FRAME_COUNT,
                            "policy.verificationFrameCount"),
                    longValue(source.get("verificationFrameSizeBytes"),
                            TransferPolicy.DEFAULT_VERIFICATION_FRAME_SIZE_BYTES,
                            "policy.verificationFrameSizeBytes"),
                    enumValue(SourceSuccessAction.class, source.get("sourceSuccessAction"),
                            SourceSuccessAction.KEEP, "policy.sourceSuccessAction"),
                    nullableString(source.get("sourceBackupRootPath")));
        } catch (TransferContractException exception) {
            throw exception;
        } catch (IllegalArgumentException exception) {
            throw invalid("policy", exception.getMessage(), exception);
        }
    }

    public static Map<String, Object> normalizePolicyMap(Map<String, ?> values) {
        Map<String, Object> normalized = new LinkedHashMap<>();
        if (values != null) {
            values.forEach(normalized::put);
        }
        TransferPolicy policy = normalizePolicy(values);
        normalized.put("conflictPolicy", policy.conflictPolicy().name());
        normalized.put("checksumAlgorithm", policy.checksumAlgorithm());
        normalized.put("verificationMode", policy.verificationMode().name());
        normalized.put("verificationFrameCount", policy.verificationFrameCount());
        normalized.put("verificationFrameSizeBytes", policy.verificationFrameSizeBytes());
        normalized.put("sourceSuccessAction", policy.sourceSuccessAction().name());
        normalized.put("sourceBackupRootPath",
                policy.sourceSuccessAction() == SourceSuccessAction.BACKUP
                        ? policy.sourceBackupRootPath()
                        : null);
        return normalized;
    }

    private static <E extends Enum<E>> E enumValue(Class<E> type, Object raw, E fallback, String field) {
        if (raw == null || String.valueOf(raw).isBlank()) {
            return fallback;
        }
        String value = String.valueOf(raw).trim().replace('-', '_').toUpperCase(Locale.ROOT);
        try {
            return Enum.valueOf(type, value);
        } catch (IllegalArgumentException exception) {
            throw invalid(field, field + " is invalid: " + value, exception);
        }
    }

    private static int integerValue(Object raw, int fallback, String field) {
        long value = longValue(raw, fallback, field);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw invalid(field, field + " is outside the integer range", null);
        }
        return (int) value;
    }

    private static long longValue(Object raw, long fallback, String field) {
        if (raw == null || String.valueOf(raw).isBlank()) {
            return fallback;
        }
        try {
            return new BigDecimal(String.valueOf(raw)).longValueExact();
        } catch (RuntimeException exception) {
            throw invalid(field, field + " must be an integer", exception);
        }
    }

    private static String stringValue(Object raw, String fallback) {
        return raw == null || String.valueOf(raw).isBlank() ? fallback : String.valueOf(raw);
    }

    private static String nullableString(Object raw) {
        return raw == null || String.valueOf(raw).isBlank() ? null : String.valueOf(raw);
    }

    private static TransferContractException invalid(String field, String message, Throwable cause) {
        return new TransferContractException("INVALID_TRANSFER_SPEC", field, message, cause);
    }
}
