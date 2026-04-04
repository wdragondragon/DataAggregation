package com.jdragon.aggregation.core.util;

import org.slf4j.MDC;

import java.util.Map;

public final class MdcTaskDecorator {

    private MdcTaskDecorator() {
    }

    public static Runnable wrap(Runnable delegate) {
        final Map<String, String> captured = MDC.getCopyOfContextMap();
        return () -> {
            final Map<String, String> previous = MDC.getCopyOfContextMap();
            apply(captured);
            try {
                delegate.run();
            } finally {
                apply(previous);
            }
        };
    }

    public static Thread newThread(Runnable delegate, String name) {
        return new Thread(wrap(delegate), name);
    }

    private static void apply(Map<String, String> context) {
        if (context == null || context.isEmpty()) {
            MDC.clear();
            return;
        }
        MDC.setContextMap(context);
    }
}
