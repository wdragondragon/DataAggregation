package com.jdragon.aggregation.plugin.httpdyn.util;

import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.junit.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HttpReadTimeoutSessionTest {

    @Test
    public void defaultsShouldReachSixtySecondsOnceAndStop() {
        HttpReadTimeoutSession session = new HttpReadTimeoutSession(0);
        List<Integer> attempts = new ArrayList<>();
        do {
            attempts.add(session.getReadTimeoutMs());
        } while (session.increaseAfterReadTimeout());

        assertEquals(Arrays.asList(10_000, 20_000, 30_000, 40_000, 50_000, 60_000), attempts);
        assertFalse(session.increaseAfterReadTimeout());
        assertEquals(60_000, session.getReadTimeoutMs());
    }

    @Test
    public void configuredTimeoutShouldBeInitialValueWithCappedLastStep() {
        HttpReadTimeoutSession session = new HttpReadTimeoutSession(25_000);
        List<Integer> attempts = new ArrayList<>();
        do {
            attempts.add(session.getReadTimeoutMs());
        } while (session.increaseAfterReadTimeout());

        assertEquals(Arrays.asList(25_000, 35_000, 45_000, 55_000, 60_000), attempts);
        assertEquals(60_000, new HttpReadTimeoutSession(Integer.MAX_VALUE).getReadTimeoutMs());
        assertEquals(10_000, new HttpReadTimeoutSession(-1).getReadTimeoutMs());
    }

    @Test
    public void newExecutionShouldNotInheritOtherExecutionTimeout() {
        HttpReadTimeoutSession first = new HttpReadTimeoutSession(0);
        first.increaseAfterReadTimeout();
        first.increaseAfterReadTimeout();

        assertEquals(30_000, first.getReadTimeoutMs());
        assertEquals(10_000, new HttpReadTimeoutSession(0).getReadTimeoutMs());
    }

    @Test
    public void onlySocketTimeoutCauseShouldCountAsReadTimeout() {
        assertTrue(HttpReadTimeoutSession.isReadTimeout(
                new IllegalStateException("outer", new IOException(new SocketTimeoutException()))));
        assertFalse(HttpReadTimeoutSession.isReadTimeout(null));
        assertFalse(HttpReadTimeoutSession.isReadTimeout(new IOException("Read timed out")));
        assertFalse(HttpReadTimeoutSession.isReadTimeout(new ConnectTimeoutException()));
        assertFalse(HttpReadTimeoutSession.isReadTimeout(new ConnectionPoolTimeoutException()));

        ConnectTimeoutException connectionFailure = new ConnectTimeoutException();
        connectionFailure.initCause(new SocketTimeoutException());
        assertFalse(HttpReadTimeoutSession.isReadTimeout(connectionFailure));
    }

    @Test
    public void causeCyclesShouldNotHangClassification() {
        IOException first = new IOException("first");
        IOException second = new IOException("second", first);
        first.initCause(second);
        assertFalse(HttpReadTimeoutSession.isReadTimeout(first));
    }
}
