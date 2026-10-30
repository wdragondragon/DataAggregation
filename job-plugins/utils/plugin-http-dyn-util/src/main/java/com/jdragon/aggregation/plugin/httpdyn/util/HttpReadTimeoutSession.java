package com.jdragon.aggregation.plugin.httpdyn.util;

import org.apache.http.conn.ConnectTimeoutException;

import java.net.SocketTimeoutException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * Read timeout state for one HTTP reader execution or scan, shared across its
 * pages. Instances must not be shared by concurrent executions.
 */
public final class HttpReadTimeoutSession {

    public static final int DEFAULT_TIMEOUT_MS = 10_000;
    public static final int TIMEOUT_INCREMENT_MS = 10_000;
    public static final int MAX_TIMEOUT_MS = 60_000;

    private int readTimeoutMs;

    public HttpReadTimeoutSession(int configuredTimeoutMs) {
        readTimeoutMs = configuredTimeoutMs > 0
                ? Math.min(configuredTimeoutMs, MAX_TIMEOUT_MS)
                : DEFAULT_TIMEOUT_MS;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    /**
     * Advances the timeout for the next attempt. A successful attempt keeps
     * this value for subsequent pages; a timeout at the cap allows no more retries.
     */
    public boolean increaseAfterReadTimeout() {
        if (readTimeoutMs >= MAX_TIMEOUT_MS) {
            return false;
        }
        readTimeoutMs = Math.min(readTimeoutMs + TIMEOUT_INCREMENT_MS, MAX_TIMEOUT_MS);
        return true;
    }

    /**
     * Looks through wrapped failures without depending on exception messages.
     * Apache connection and pool timeouts must never increase the read timeout.
     */
    public static boolean isReadTimeout(Throwable failure) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        boolean socketTimeout = false;
        for (Throwable cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
            if (cause instanceof ConnectTimeoutException) {
                return false;
            }
            if (cause instanceof SocketTimeoutException) {
                socketTimeout = true;
            }
        }
        return socketTimeout;
    }
}
