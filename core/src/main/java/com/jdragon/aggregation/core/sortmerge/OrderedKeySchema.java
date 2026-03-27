package com.jdragon.aggregation.core.sortmerge;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class OrderedKeySchema {

    private static final String[] DATE_PATTERNS = new String[]{
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss.SSS",
            "yyyy-MM-dd",
            "yyyy/MM/dd HH:mm:ss",
            "yyyy/MM/dd"
    };

    private final List<String> keyFields;
    private final Map<String, OrderedKeyType> configuredTypes;
    private final Map<String, OrderedKeyType> resolvedTypes = new java.util.LinkedHashMap<String, OrderedKeyType>();

    public OrderedKeySchema(List<String> keyFields, Map<String, OrderedKeyType> configuredTypes) {
        this.keyFields = keyFields != null ? keyFields : new ArrayList<String>();
        this.configuredTypes = configuredTypes != null ? configuredTypes : new java.util.LinkedHashMap<String, OrderedKeyType>();
    }

    public int compare(OrderedKey left, OrderedKey right) {
        for (int i = 0; i < keyFields.size(); i++) {
            String field = keyFields.get(i);
            Object leftValue = i < left.getValues().size() ? left.getValues().get(i) : null;
            Object rightValue = i < right.getValues().size() ? right.getValues().get(i) : null;
            int result = compareValue(field, leftValue, rightValue);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    private int compareValue(String field, Object left, Object right) {
        if (left == right) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }

        OrderedKeyType type = resolveType(field, left, right);
        switch (type) {
            case NUMBER:
                return toBigDecimal(field, left).compareTo(toBigDecimal(field, right));
            case DATETIME:
                return Long.compare(toEpochMillis(field, left), toEpochMillis(field, right));
            case STRING:
            case AUTO:
            default:
                return String.valueOf(left).compareTo(String.valueOf(right));
        }
    }

    private OrderedKeyType resolveType(String field, Object left, Object right) {
        OrderedKeyType configured = configuredTypes.get(field);
        if (configured != null && configured != OrderedKeyType.AUTO) {
            return configured;
        }
        OrderedKeyType resolved = resolvedTypes.get(field);
        OrderedKeyType inferred = inferType(left, right);
        if (resolved == null) {
            resolvedTypes.put(field, inferred);
            return inferred;
        }
        if (resolved == inferred || inferred == OrderedKeyType.STRING) {
            return resolved;
        }
        if (resolved == OrderedKeyType.STRING) {
            resolvedTypes.put(field, inferred);
            return inferred;
        }
        if (!isCompatible(resolved, left) || !isCompatible(resolved, right)) {
            throw new IllegalStateException("Incompatible key type detected for field: " + field);
        }
        return resolved;
    }

    private OrderedKeyType inferType(Object left, Object right) {
        Object sample = left != null ? left : right;
        if (sample == null) {
            return OrderedKeyType.STRING;
        }
        if (sample instanceof Number) {
            return OrderedKeyType.NUMBER;
        }
        if (sample instanceof Date || sample instanceof Timestamp
                || sample instanceof Instant || sample instanceof LocalDateTime
                || sample instanceof OffsetDateTime || sample instanceof ZonedDateTime) {
            return OrderedKeyType.DATETIME;
        }
        String value = String.valueOf(sample).trim();
        if (value.isEmpty()) {
            return OrderedKeyType.STRING;
        }
        try {
            new BigDecimal(value);
            return OrderedKeyType.NUMBER;
        } catch (NumberFormatException ignored) {
        }
        if (parseDate(value) != null) {
            return OrderedKeyType.DATETIME;
        }
        return OrderedKeyType.STRING;
    }

    private boolean isCompatible(OrderedKeyType type, Object value) {
        if (value == null) {
            return true;
        }
        try {
            switch (type) {
                case NUMBER:
                    toBigDecimal("", value);
                    return true;
                case DATETIME:
                    toEpochMillis("", value);
                    return true;
                case STRING:
                case AUTO:
                default:
                    return true;
            }
        } catch (RuntimeException ex) {
            return false;
        }
    }

    private BigDecimal toBigDecimal(String field, Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return new BigDecimal(String.valueOf(value));
        }
        try {
            return new BigDecimal(String.valueOf(value).trim());
        } catch (NumberFormatException ex) {
            throw new IllegalStateException("Failed to convert key field to number: " + field);
        }
    }

    private long toEpochMillis(String field, Object value) {
        if (value instanceof Date) {
            return ((Date) value).getTime();
        }
        if (value instanceof Timestamp) {
            return ((Timestamp) value).getTime();
        }
        if (value instanceof Instant) {
            return ((Instant) value).toEpochMilli();
        }
        if (value instanceof LocalDateTime) {
            return ((LocalDateTime) value).atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        }
        if (value instanceof OffsetDateTime) {
            return ((OffsetDateTime) value).toInstant().toEpochMilli();
        }
        if (value instanceof ZonedDateTime) {
            return ((ZonedDateTime) value).toInstant().toEpochMilli();
        }
        Date date = parseDate(String.valueOf(value));
        if (date != null) {
            return date.getTime();
        }
        throw new IllegalStateException("Failed to convert key field to datetime: " + field);
    }

    private Date parseDate(String value) {
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        try {
            return Date.from(Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(value)));
        } catch (DateTimeParseException ignored) {
        }
        try {
            return Date.from(OffsetDateTime.parse(value).toInstant());
        } catch (DateTimeParseException ignored) {
        }
        for (String pattern : DATE_PATTERNS) {
            try {
                return new SimpleDateFormat(pattern).parse(value);
            } catch (ParseException ignored) {
            }
        }
        return null;
    }
}
