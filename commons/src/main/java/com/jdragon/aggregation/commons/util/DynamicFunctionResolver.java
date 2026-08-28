package com.jdragon.aggregation.commons.util;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Shared implementation of the dynamic functions used by collection and transfer jobs. */
public final class DynamicFunctionResolver {

    public enum Mode { STRICT, COMPATIBLE }

    private static final Pattern FUNCTION_PATTERN = Pattern.compile("\\$([A-Za-z_][A-Za-z0-9_]*)\\(([^()]*)\\)");
    private static final Pattern FUNCTION_TOKEN = Pattern.compile("\\$[A-Za-z_][A-Za-z0-9_]*");

    private DynamicFunctionResolver() {
    }

    public static String replaceAll(String content, LocalDateTime baseTime, Mode mode) {
        return replaceAll(content, baseTime, mode, ZoneId.systemDefault());
    }

    /**
     * Resolves functions using a local base time and the zone used to interpret
     * epoch-millisecond arguments.
     */
    public static String replaceAll(String content, LocalDateTime baseTime, Mode mode, ZoneId zoneId) {
        if (content == null || content.trim().isEmpty()) {
            return content;
        }
        LocalDateTime actualBaseTime = baseTime == null ? LocalDateTime.now() : baseTime;
        ZoneId actualZoneId = zoneId == null ? ZoneId.systemDefault() : zoneId;
        if (mode == Mode.STRICT) {
            validateTemplateFunctionNesting(content);
        }
        Matcher matcher = FUNCTION_PATTERN.matcher(content);
        StringBuffer buffer = new StringBuffer();
        Set<String> matchedFunctions = new LinkedHashSet<>();
        while (matcher.find()) {
            String expression = matcher.group();
            matchedFunctions.add(expression);
            String args = matcher.group(2);
            if (args.contains("${")) {
                throw new IllegalArgumentException("dynamic function and template placeholders cannot be nested: " + expression);
            }
            String replacement;
            try {
                replacement = execute(matcher.group(1), splitArgs(args, mode == Mode.STRICT), actualBaseTime,
                        actualZoneId, mode);
            } catch (RuntimeException exception) {
                if (mode == Mode.COMPATIBLE && !isKnownFunction(matcher.group(1))) {
                    replacement = "";
                } else {
                    throw new IllegalArgumentException("invalid dynamic function " + expression + ": "
                            + message(exception), exception);
                }
            }
            matcher.appendReplacement(buffer, Matcher.quoteReplacement(replacement));
        }
        matcher.appendTail(buffer);
        if (mode == Mode.STRICT) {
            validateUnmatchedFunctionTokens(content, matchedFunctions);
        }
        return buffer.toString();
    }

    public static String replaceAll(String content, LocalDateTime baseTime) {
        return replaceAll(content, baseTime, Mode.STRICT);
    }

    public static String replaceAll(String content) {
        return replaceAll(content, LocalDateTime.now(), Mode.STRICT);
    }

    public static List<String> validate(String content, Mode mode) {
        List<String> errors = new ArrayList<>();
        if (content == null || content.trim().isEmpty()) {
            return errors;
        }
        try {
            replaceAll(content, LocalDateTime.now(), mode);
        } catch (RuntimeException exception) {
            errors.add(exception.getMessage() == null ? exception.toString() : exception.getMessage());
        }
        return errors;
    }

    private static void validateUnmatchedFunctionTokens(String content, Set<String> matchedFunctions) {
        Matcher tokenMatcher = FUNCTION_TOKEN.matcher(content);
        while (tokenMatcher.find()) {
            String token = tokenMatcher.group();
            boolean covered = false;
            for (String expression : matchedFunctions) {
                if (expression.startsWith(token) && content.indexOf(expression, tokenMatcher.start()) == tokenMatcher.start()) {
                    covered = true;
                    break;
                }
            }
            if (!covered) {
                throw new IllegalArgumentException("invalid or unsupported dynamic function expression: " + token);
            }
        }
    }

    private static void validateTemplateFunctionNesting(String content) {
        int cursor = 0;
        while (cursor < content.length()) {
            int start = content.indexOf("${", cursor);
            if (start < 0) {
                return;
            }
            int end = content.indexOf('}', start + 2);
            if (end < 0) {
                throw new IllegalArgumentException("invalid template expression: unclosed expression");
            }
            String body = content.substring(start + 2, end);
            if (body.matches(".*\\$[A-Za-z_][A-Za-z0-9_]*\\(.*")) {
                throw new IllegalArgumentException("dynamic functions cannot be nested inside template variables");
            }
            cursor = end + 1;
        }
    }

    private static boolean isKnownFunction(String name) {
        return switch (name) {
            case "getCurrentTime", "getTheMonthLastDay", "getTimeUnitValue", "subStr", "subString", "toLower", "toUpper" -> true;
            default -> false;
        };
    }

    private static String execute(String functionName, String[] args, LocalDateTime baseTime, ZoneId zoneId,
                                  Mode mode) {
        return switch (functionName) {
            case "getCurrentTime" -> getCurrentTime(args, baseTime, mode);
            case "getTheMonthLastDay" -> getTheMonthLastDay(args, baseTime, zoneId, mode);
            case "getTimeUnitValue" -> getTimeUnitValue(args, baseTime, zoneId, mode);
            case "subStr" -> subStr(args);
            case "subString" -> subString(args);
            case "toLower" -> toLower(args);
            case "toUpper" -> toUpper(args);
            default -> throw new IllegalArgumentException("unsupported dynamic function: " + functionName);
        };
    }

    private static String getCurrentTime(String[] args, LocalDateTime baseTime, Mode mode) {
        requireMinArgCount("getCurrentTime", args, 1);
        String pattern = stripQuotes(args[0]);
        if (pattern.isBlank()) {
            throw new IllegalArgumentException("getCurrentTime pattern is required");
        }
        LocalDateTime value = baseTime;
        if (args.length > 1 && !stripQuotes(args[1]).isBlank()) {
            value = applyOffset(value, stripQuotes(args[1]).trim());
        }
        if (mode == Mode.STRICT && args.length > 2) {
            throw new IllegalArgumentException("getCurrentTime accepts at most 2 parameters");
        }
        return DateTimeFormatter.ofPattern(pattern).format(value);
    }

    private static String getTheMonthLastDay(String[] args, LocalDateTime baseTime, ZoneId zoneId, Mode mode) {
        if (mode == Mode.STRICT && args.length > 1) {
            throw new IllegalArgumentException("getTheMonthLastDay accepts at most 1 parameter");
        }
        LocalDateTime value = args.length == 0 || stripQuotes(args[0]).isBlank()
                ? baseTime : parseDateTime(stripQuotes(args[0]), baseTime, zoneId);
        return String.valueOf(YearMonth.from(value).lengthOfMonth());
    }

    private static String getTimeUnitValue(String[] args, LocalDateTime baseTime, ZoneId zoneId, Mode mode) {
        requireArgCount("getTimeUnitValue", args, 2);
        LocalDateTime value = parseDateTime(stripQuotes(args[0]), baseTime, zoneId);
        String unit = stripQuotes(args[1]).toLowerCase(Locale.ENGLISH);
        return switch (unit) {
            case "y" -> String.valueOf(value.getYear());
            case "m" -> String.valueOf(value.getMonthValue());
            case "d" -> String.valueOf(value.getDayOfMonth());
            case "h" -> String.valueOf(value.getHour());
            case "mi" -> String.valueOf(value.getMinute());
            case "s" -> String.valueOf(value.getSecond());
            case "day" -> String.valueOf(value.getDayOfWeek().getValue());
            default -> unsupportedUnitResult(unit, mode);
        };
    }

    private static String unsupportedUnitResult(String unit, Mode mode) {
        if (mode == Mode.COMPATIBLE) {
            return "0";
        }
        throw new IllegalArgumentException("unsupported unit for getTimeUnitValue: " + unit);
    }

    private static String subStr(String[] args) {
        requireArgCount("subStr", args, 3);
        String source = stripQuotes(args[0]);
        int start = parseInt(stripQuotes(args[1]));
        int length = parseInt(stripQuotes(args[2]));
        if (args.length != 3 || source.isEmpty()) {
            if (args.length != 3) throw new IllegalArgumentException("subStr requires exactly 3 parameters");
            return "";
        }
        int from = Math.max(0, start);
        int to = Math.min(source.length(), from + Math.max(0, length));
        return from >= to ? "" : source.substring(from, to);
    }

    private static String subString(String[] args) {
        requireArgCount("subString", args, 3);
        if (args.length != 3) throw new IllegalArgumentException("subString requires exactly 3 parameters");
        String source = stripQuotes(args[0]);
        int from = Math.max(0, parseInt(stripQuotes(args[1])));
        int to = Math.min(source.length(), Math.max(from, parseInt(stripQuotes(args[2]))));
        return from >= to ? "" : source.substring(from, to);
    }

    private static String toLower(String[] args) {
        requireArgCount("toLower", args, 1);
        if (args.length != 1) throw new IllegalArgumentException("toLower requires exactly 1 parameter");
        return stripQuotes(args[0]).toLowerCase(Locale.ENGLISH);
    }

    private static String toUpper(String[] args) {
        requireArgCount("toUpper", args, 1);
        if (args.length != 1) throw new IllegalArgumentException("toUpper requires exactly 1 parameter");
        return stripQuotes(args[0]).toUpperCase(Locale.ENGLISH);
    }

    private static LocalDateTime parseDateTime(String value, LocalDateTime fallback, ZoneId zoneId) {
        if (value == null || value.trim().isEmpty()) return fallback;
        String trimmed = value.trim();
        if (trimmed.matches("^-?\\d+$")) {
            long numeric = Long.parseLong(trimmed);
            if (trimmed.length() == 8) return LocalDateTime.parse(trimmed + "000000", DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            if (trimmed.length() == 14) return LocalDateTime.parse(trimmed, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            if (trimmed.length() >= 13) return LocalDateTime.ofInstant(Instant.ofEpochMilli(numeric), zoneId);
        }
        for (DateTimeFormatter formatter : List.of(
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))) {
            try { return LocalDateTime.parse(trimmed, formatter); } catch (DateTimeParseException ignored) { }
        }
        try { return LocalDateTime.parse(trimmed + " 00:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")); }
        catch (DateTimeParseException exception) { throw new IllegalArgumentException("unable to parse datetime value: " + value, exception); }
    }

    private static LocalDateTime applyOffset(LocalDateTime base, String offset) {
        String normalized = offset.replace(" ", "");
        if (normalized.matches(".*[ymdhisw].*")) {
            Matcher matcher = Pattern.compile("([+-]?)([^+-]+?)(mi|[ymdhisw])").matcher(normalized);
            LocalDateTime current = base;
            int consumed = 0;
            while (matcher.find()) {
                long value = evaluateSecondExpression(matcher.group(2));
                if ("-".equals(matcher.group(1))) value = -value;
                current = applyTemporalUnit(current, value, matcher.group(3));
                consumed += matcher.group().length();
            }
            if (consumed != normalized.length()) throw new IllegalArgumentException("unsupported offset expression: " + offset);
            return current;
        }
        return base.plusSeconds(evaluateSecondExpression(normalized));
    }

    private static LocalDateTime applyTemporalUnit(LocalDateTime value, long amount, String unit) {
        return switch (unit) {
            case "y" -> value.plusYears(amount);
            case "m" -> value.plusMonths(amount);
            case "w" -> value.plusWeeks(amount);
            case "d" -> value.plusDays(amount);
            case "h" -> value.plusHours(amount);
            case "mi", "i" -> value.plusMinutes(amount);
            case "s" -> value.plus(amount, ChronoUnit.SECONDS);
            default -> throw new IllegalArgumentException("unsupported offset unit: " + unit);
        };
    }

    private static long evaluateSecondExpression(String expression) {
        String normalized = expression.replace(" ", "");
        if (normalized.isEmpty()) return 0L;
        List<String> terms = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        for (int index = 0; index < normalized.length(); index++) {
            char character = normalized.charAt(index);
            if ((character == '+' || character == '-') && current.length() > 0) {
                terms.add(current.toString()); current.setLength(0);
            }
            current.append(character);
        }
        terms.add(current.toString());
        long result = 0L;
        for (String term : terms) result += multiplyFactors(term);
        return result;
    }

    private static long multiplyFactors(String term) {
        boolean negative = term.startsWith("-");
        String normalized = negative || term.startsWith("+") ? term.substring(1) : term;
        long result = 1L;
        for (String factor : normalized.split("\\*")) {
            if (!factor.isBlank()) result *= Long.parseLong(factor);
        }
        return negative ? -result : result;
    }

    private static String[] splitArgs(String value, boolean strict) {
        if (value == null || value.trim().isEmpty()) return new String[0];
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean quoted = false;
        char quote = 0;
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (character == '\'' || character == '"') {
                if (!quoted) { quoted = true; quote = character; }
                else if (quote == character) { quoted = false; quote = 0; }
            }
            if (character == ',' && !quoted) { parts.add(current.toString().trim()); current.setLength(0); }
            else current.append(character);
        }
        if (quoted && strict) {
            throw new IllegalArgumentException("unterminated quoted parameter");
        }
        parts.add(current.toString().trim());
        return parts.toArray(new String[0]);
    }

    private static String stripQuotes(String value) {
        if (value == null) return "";
        String trimmed = value.trim();
        if (trimmed.length() >= 2 && ((trimmed.startsWith("'") && trimmed.endsWith("'"))
                || (trimmed.startsWith("\"") && trimmed.endsWith("\"")))) return trimmed.substring(1, trimmed.length() - 1);
        return trimmed;
    }

    private static int parseInt(String value) {
        try { return Integer.parseInt(value); }
        catch (NumberFormatException exception) { throw new IllegalArgumentException("invalid integer value: " + value, exception); }
    }

    private static void requireArgCount(String name, String[] args, int count) {
        if (args == null || args.length != count) throw new IllegalArgumentException(name + " requires exactly " + count + " parameters");
    }

    private static void requireMinArgCount(String name, String[] args, int count) {
        if (args == null || args.length < count) throw new IllegalArgumentException(name + " requires at least " + count + " parameters");
    }

    private static String message(RuntimeException exception) {
        return exception.getMessage() == null ? exception.toString() : exception.getMessage();
    }
}
