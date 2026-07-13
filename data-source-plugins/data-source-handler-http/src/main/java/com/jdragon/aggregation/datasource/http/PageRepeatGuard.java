package com.jdragon.aggregation.datasource.http;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

public final class PageRepeatGuard {
    private static final int MAX_TRACKED_PAGE_SIGNATURES = 256;
    private final int maxRepeatedPages;
    private final Set<String> observedSignatures = new LinkedHashSet<>();
    private int repeatedPages;

    public PageRepeatGuard(int maxRepeatedPages) {
        this.maxRepeatedPages = maxRepeatedPages;
    }

    public boolean observe(String pageContent) {
        if (maxRepeatedPages <= 0) {
            return false;
        }
        String currentSignature = signature(pageContent);
        if (!observedSignatures.add(currentSignature)) {
            repeatedPages++;
            return repeatedPages >= maxRepeatedPages;
        }
        repeatedPages = 0;
        if (observedSignatures.size() > MAX_TRACKED_PAGE_SIGNATURES) {
            Iterator<String> iterator = observedSignatures.iterator();
            iterator.next();
            iterator.remove();
        }
        return false;
    }

    private String signature(String pageContent) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(String.valueOf(pageContent).getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }
}
