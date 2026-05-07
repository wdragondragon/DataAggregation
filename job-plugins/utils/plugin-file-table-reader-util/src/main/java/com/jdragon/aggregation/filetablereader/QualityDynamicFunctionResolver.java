package com.jdragon.aggregation.filetablereader;

import org.apache.commons.lang3.StringUtils;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class QualityDynamicFunctionResolver {

    private static final Pattern FUNCTION_PATTERN = Pattern.compile("\\$([A-Za-z_][A-Za-z0-9_]*)\\(([^()]*)\\)");

    private QualityDynamicFunctionResolver() {
    }

    static String replaceAll(String content) {
        if (StringUtils.isBlank(content)) {
            return content;
        }
        Matcher matcher = FUNCTION_PATTERN.matcher(content);
        StringBuffer buffer = new StringBuffer();
        while (matcher.find()) {
            String replacement = execute(matcher.group(1), splitArgs(matcher.group(2)));
            matcher.appendReplacement(buffer, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(buffer);
        return buffer.toString();
    }

    private static String execute(String functionName, String[] args) {
        if ("getCurrentTime".equals(functionName)) {
            return getCurrentTime(args);
        }
        if ("getTheMonthLastDay".equals(functionName)) {
            return getTheMonthLastDay(args);
        }
        if ("getTimeUnitValue".equals(functionName)) {
            return getTimeUnitValue(args);
        }
        if ("subStr".equals(functionName)) {
            return subStr(args);
        }
        if ("subString".equals(functionName)) {
            return subString(args);
        }
        if ("toLower".equals(functionName)) {
            return toLower(args);
        }
        if ("toUpper".equals(functionName)) {
            return toUpper(args);
        }
        return "";
    }

    private static String getCurrentTime(String[] args) {
        requireArgCount("getCurrentTime", args, 2);
        String pattern = stripQuotes(args[0]);
        String offset = stripQuotes(args[1]);
        LocalDateTime value = LocalDateTime.now();
        if (StringUtils.isNotBlank(offset)) {
            value = applyOffset(value, offset.trim());
        }
        return DateTimeFormatter.ofPattern(pattern).format(value);
    }

    private static String getTheMonthLastDay(String[] args) {
        requireArgCount("getTheMonthLastDay", args, 1);
        String value = stripQuotes(args[0]);
        if (!value.matches("^-?\\d+$")) {
            throw new IllegalArgumentException("getTheMonthLastDay parameter must be timestamp millis: " + value);
        }
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(value)), ZoneId.systemDefault());
        return DateTimeFormatter.ofPattern("dd").format(YearMonth.from(dateTime).atEndOfMonth());
    }

    private static String getTimeUnitValue(String[] args) {
        requireArgCount("getTimeUnitValue", args, 2);
        LocalDateTime value = LocalDateTime.parse(stripQuotes(args[0]), DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        String unit = stripQuotes(args[1]).toLowerCase(Locale.ENGLISH);
        if ("y".equals(unit)) {
            return String.valueOf(value.getYear());
        }
        if ("m".equals(unit)) {
            return String.valueOf(value.getMonthValue());
        }
        if ("d".equals(unit)) {
            return String.valueOf(value.getDayOfMonth());
        }
        if ("h".equals(unit)) {
            return String.valueOf(value.getHour());
        }
        if ("mi".equals(unit)) {
            return String.valueOf(value.getMinute());
        }
        if ("s".equals(unit)) {
            return String.valueOf(value.getSecond());
        }
        if ("day".equals(unit)) {
            DayOfWeek dayOfWeek = value.getDayOfWeek();
            return String.valueOf(dayOfWeek.getValue());
        }
        return "0";
    }

    private static String subStr(String[] args) {
        requireArgCount("subStr", args, 3);
        String source = stripQuotes(args[0]);
        int beginIndex = Integer.parseInt(stripQuotes(args[1]));
        int endIndex = Integer.parseInt(stripQuotes(args[2])) + 1;
        return source.substring(beginIndex, endIndex);
    }

    private static String subString(String[] args) {
        requireArgCount("subString", args, 3);
        String source = stripQuotes(args[0]);
        int beginIndex = Integer.parseInt(stripQuotes(args[1]));
        int endIndex = Integer.parseInt(stripQuotes(args[2]));
        return source.substring(beginIndex, endIndex);
    }

    private static String toLower(String[] args) {
        requireArgCount("toLower", args, 1);
        return stripQuotes(args[0]).toLowerCase(Locale.ENGLISH);
    }

    private static String toUpper(String[] args) {
        requireArgCount("toUpper", args, 1);
        return stripQuotes(args[0]).toUpperCase(Locale.ENGLISH);
    }

    private static LocalDateTime applyOffset(LocalDateTime base, String offset) {
        if (offset.matches(".*[ymdhisw].*")) {
            Matcher matcher = Pattern.compile("([+-]?)([^+-]+?)(mi|[ymdhisw])").matcher(offset.replace(" ", ""));
            LocalDateTime current = base;
            int consumed = 0;
            while (matcher.find()) {
                String sign = matcher.group(1);
                long value = evaluateSecondExpression(matcher.group(2));
                if ("-".equals(sign)) {
                    value = -value;
                }
                current = applyTemporalUnit(current, value, matcher.group(3));
                consumed += matcher.group().length();
            }
            if (consumed != offset.replace(" ", "").length()) {
                throw new IllegalArgumentException("Unsupported offset expression: " + offset);
            }
            return current;
        }
        return base.plusSeconds(evaluateSecondExpression(offset));
    }

    private static LocalDateTime applyTemporalUnit(LocalDateTime value, long amount, String unit) {
        if ("y".equals(unit)) {
            return value.plusYears(amount);
        }
        if ("m".equals(unit)) {
            return value.plusMonths(amount);
        }
        if ("d".equals(unit)) {
            return value.plusDays(amount);
        }
        if ("h".equals(unit)) {
            return value.plusHours(amount);
        }
        if ("mi".equals(unit) || "i".equals(unit)) {
            return value.plusMinutes(amount);
        }
        if ("s".equals(unit)) {
            return value.plus(amount, ChronoUnit.SECONDS);
        }
        if ("w".equals(unit)) {
            return value.plusWeeks(amount);
        }
        throw new IllegalArgumentException("Unsupported offset unit: " + unit);
    }

    private static long evaluateSecondExpression(String expression) {
        String normalized = expression.replace(" ", "");
        if (normalized.isEmpty()) {
            return 0L;
        }
        List<String> terms = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (int index = 0; index < normalized.length(); index++) {
            char currentChar = normalized.charAt(index);
            if ((currentChar == '+' || currentChar == '-') && current.length() > 0) {
                terms.add(current.toString());
                current.setLength(0);
            }
            current.append(currentChar);
        }
        terms.add(current.toString());
        long result = 0L;
        for (String term : terms) {
            if (StringUtils.isBlank(term)) {
                continue;
            }
            result += multiplyFactors(term);
        }
        return result;
    }

    private static long multiplyFactors(String term) {
        boolean negative = term.startsWith("-");
        String normalized = negative || term.startsWith("+") ? term.substring(1) : term;
        long result = 1L;
        String[] factors = normalized.split("\\*");
        for (String factor : factors) {
            if (StringUtils.isBlank(factor)) {
                continue;
            }
            result *= Long.parseLong(factor.trim());
        }
        return negative ? -result : result;
    }

    private static String[] splitArgs(String value) {
        if (StringUtils.isBlank(value)) {
            return new String[0];
        }
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = 0;
        for (int index = 0; index < value.length(); index++) {
            char currentChar = value.charAt(index);
            if (currentChar == '\'' || currentChar == '"') {
                if (!inQuotes) {
                    inQuotes = true;
                    quoteChar = currentChar;
                } else if (quoteChar == currentChar) {
                    inQuotes = false;
                    quoteChar = 0;
                }
            }
            if (currentChar == ',' && !inQuotes) {
                parts.add(current.toString().trim());
                current.setLength(0);
                continue;
            }
            current.append(currentChar);
        }
        parts.add(current.toString().trim());
        return parts.toArray(new String[0]);
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

    private static void requireArgCount(String functionName, String[] args, int count) {
        if (args == null || args.length != count) {
            throw new IllegalArgumentException(functionName + " requires " + count + " parameters");
        }
    }
}
