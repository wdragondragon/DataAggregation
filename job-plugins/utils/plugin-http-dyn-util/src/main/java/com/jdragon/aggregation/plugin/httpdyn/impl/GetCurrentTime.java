package com.jdragon.aggregation.plugin.httpdyn.impl;

import com.jdragon.aggregation.plugin.httpdyn.BaseHttpDynExecutor;

import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Formats the current local date-time after applying an optional offset.
 *
 * <p>The expression intentionally mirrors the quality SQL getCurrentTime
 * function so the same format and offset strings can be reused in HTTP
 * reader/writer options.</p>
 */
public class GetCurrentTime extends BaseHttpDynExecutor {

    private static final Pattern OFFSET_WITH_UNITS =
            Pattern.compile("([+-]?)(\\d+)(mi|[ymwdhs])");

    private final Clock clock;

    public GetCurrentTime() {
        this(Clock.systemDefaultZone());
    }

    GetCurrentTime(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
        super.setHttpDynName("dyn_getCurrentTime");
    }

    @Override
    public String execute(Object... paras) {
        requirePattern(paras);
        String pattern = stripQuotes(String.valueOf(paras[0]));
        String offset = paras.length > 1 && paras[1] != null
                ? stripQuotes(String.valueOf(paras[1]))
                : "";

        LocalDateTime value = LocalDateTime.now(clock);
        if (!offset.trim().isEmpty()) {
            value = applyOffset(value, offset.trim());
        }
        return DateTimeFormatter.ofPattern(pattern).format(value);
    }

    private static void requirePattern(Object[] paras) {
        if (paras == null || paras.length == 0 || paras[0] == null
                || stripQuotes(String.valueOf(paras[0])).trim().isEmpty()) {
            throw new IllegalArgumentException("dyn_getCurrentTime requires at least 1 parameter");
        }
    }

    private static String stripQuotes(String value) {
        if (value == null) {
            return "";
        }
        String trimmed = value.trim();
        if (trimmed.length() >= 2) {
            char first = trimmed.charAt(0);
            char last = trimmed.charAt(trimmed.length() - 1);
            if ((first == '\'' && last == '\'') || (first == '"' && last == '"')) {
                return trimmed.substring(1, trimmed.length() - 1);
            }
        }
        return trimmed;
    }

    private static LocalDateTime applyOffset(LocalDateTime base, String offset) {
        String normalized = offset.replace(" ", "");
        Matcher matcher = OFFSET_WITH_UNITS.matcher(normalized);
        if (!matcher.find()) {
            return base.plusSeconds(evaluateSecondExpression(normalized));
        }

        LocalDateTime current = base;
        int consumed = 0;
        do {
            String sign = matcher.group(1);
            long amount = Long.parseLong(matcher.group(2));
            if ("-".equals(sign)) {
                amount = -amount;
            }
            current = applyTemporalUnit(current, amount, matcher.group(3));
            consumed += matcher.group().length();
        } while (matcher.find());

        if (consumed != normalized.length()) {
            throw new IllegalArgumentException("Unsupported offset expression: " + offset);
        }
        return current;
    }

    private static LocalDateTime applyTemporalUnit(LocalDateTime value, long amount, String unit) {
        switch (unit) {
            case "y":
                return value.plusYears(amount);
            case "m":
                return value.plusMonths(amount);
            case "w":
                return value.plusWeeks(amount);
            case "d":
                return value.plusDays(amount);
            case "h":
                return value.plusHours(amount);
            case "mi":
                return value.plusMinutes(amount);
            case "s":
                return value.plus(amount, ChronoUnit.SECONDS);
            default:
                throw new IllegalArgumentException("Unsupported offset unit: " + unit);
        }
    }

    private static long evaluateSecondExpression(String expression) {
        if (expression.isEmpty()) {
            return 0L;
        }
        List<String> terms = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (int index = 0; index < expression.length(); index++) {
            char currentChar = expression.charAt(index);
            if ((currentChar == '+' || currentChar == '-') && current.length() > 0) {
                terms.add(current.toString());
                current.setLength(0);
            }
            current.append(currentChar);
        }
        terms.add(current.toString());

        long result = 0L;
        for (String term : terms) {
            if (!term.isEmpty()) {
                result += multiplyFactors(term);
            }
        }
        return result;
    }

    private static long multiplyFactors(String term) {
        boolean negative = term.startsWith("-");
        String normalized = negative || term.startsWith("+") ? term.substring(1) : term;
        if (normalized.isEmpty()) {
            throw new IllegalArgumentException("Unsupported offset expression: " + term);
        }

        long result = 1L;
        String[] factors = normalized.split("\\*", -1);
        for (String factor : factors) {
            if (factor.trim().isEmpty()) {
                throw new IllegalArgumentException("Unsupported offset expression: " + term);
            }
            try {
                result *= Long.parseLong(factor.trim());
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("Unsupported offset expression: " + term, ex);
            }
        }
        return negative ? -result : result;
    }
}
