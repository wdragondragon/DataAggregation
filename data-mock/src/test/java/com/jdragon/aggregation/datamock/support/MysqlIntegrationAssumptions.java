package com.jdragon.aggregation.datamock.support;

import org.junit.Assume;

import java.util.regex.Pattern;

public final class MysqlIntegrationAssumptions {

    public static final String MYSQL_INTEGRATION_FLAG = "runDataMockMysqlIntegrationTests";

    private MysqlIntegrationAssumptions() {
    }

    public static void assumeEnabled(Class<?> testClass) {
        boolean enabled = Boolean.getBoolean(MYSQL_INTEGRATION_FLAG) || isTargetedBySurefire(testClass);
        Assume.assumeTrue("set -D" + MYSQL_INTEGRATION_FLAG
                + "=true to run DataAggregation MySQL integration tests, or target "
                + testClass.getSimpleName() + " via -Dtest", enabled);
    }

    private static boolean isTargetedBySurefire(Class<?> testClass) {
        String rawPattern = System.getProperty("test");
        if (rawPattern == null || rawPattern.trim().isEmpty()) {
            return false;
        }
        String[] selectors = rawPattern.split(",");
        for (String selector : selectors) {
            String candidate = selector == null ? "" : selector.trim();
            if (candidate.isEmpty()) {
                continue;
            }
            int methodSeparator = candidate.indexOf('#');
            if (methodSeparator >= 0) {
                candidate = candidate.substring(0, methodSeparator).trim();
            }
            if (candidate.isEmpty()) {
                continue;
            }
            if (candidate.equals(testClass.getSimpleName())
                    || candidate.equals(testClass.getName())
                    || wildcardMatches(candidate, testClass.getSimpleName())
                    || wildcardMatches(candidate, testClass.getName())) {
                return true;
            }
        }
        return false;
    }

    private static boolean wildcardMatches(String selector, String className) {
        if (selector.indexOf('*') < 0 && selector.indexOf('?') < 0) {
            return false;
        }
        String regex = selector
                .replace(".", "\\.")
                .replace("*", ".*")
                .replace("?", ".");
        return Pattern.matches(regex, className);
    }
}
