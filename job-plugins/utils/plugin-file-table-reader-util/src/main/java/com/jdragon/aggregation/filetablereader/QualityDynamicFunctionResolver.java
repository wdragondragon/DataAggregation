package com.jdragon.aggregation.filetablereader;

import com.jdragon.aggregation.commons.util.DynamicFunctionResolver;

import java.time.LocalDateTime;

/** Compatibility facade retained for existing file-table reader integrations. */
public final class QualityDynamicFunctionResolver {

    private QualityDynamicFunctionResolver() {
    }

    public static String replaceAll(String content) {
        return DynamicFunctionResolver.replaceAll(
                content, LocalDateTime.now(), DynamicFunctionResolver.Mode.COMPATIBLE);
    }
}
