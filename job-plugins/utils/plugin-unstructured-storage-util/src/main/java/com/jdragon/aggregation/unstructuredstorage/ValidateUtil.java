package com.jdragon.aggregation.unstructuredstorage;

import com.jdragon.aggregation.commons.element.*;
import com.jdragon.aggregation.commons.exception.AggregationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Date;

/**
 * <p>
 * <p>
 * </p>
 *
 * @author lxs
 * @since 2020/3/31
 */
public class ValidateUtil {
    private static final Logger log = LoggerFactory.getLogger(ValidateUtil.class);
    private static final ZoneId DEFAULT_ZONE_ID = ZoneId.systemDefault();
    private static final DateTimeFormatter FLEXIBLE_LOCAL_DATE_TIME = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd'T'HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
            .optionalEnd()
            .toFormatter();
    private static final DateTimeFormatter LOCAL_DATE_TIME_WITHOUT_SECONDS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

    public static Column validate(String name , String columnType, Object columnValue){
        Column columnGenerated = null;
        Type type = Type.valueOf(columnType.toUpperCase());
        switch (type) {
            case STRING:
            case TEXT:
                if (columnValue==null || columnValue.equals("")){
                    columnGenerated = new StringColumn();
                }else {
                    columnGenerated = new StringColumn(String.valueOf(columnValue));
                }
                break;
            case LONG:
            case NUMBER:
                try {
                    if (columnValue==null || columnValue.equals("")){
                        columnGenerated = new LongColumn();
                    }else if (isDecimalText(columnValue)) {
                        columnGenerated = new DoubleColumn(String.valueOf(columnValue).trim());
                    }else {
                        columnGenerated = new LongColumn(String.valueOf(columnValue));
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format(
                            "字段[%s]类型转换错误, 无法将[%s] 转换为[%s]",name, columnValue,
                            "LONG"));
                }
                break;
            case DECIMAL:
                try {
                    if (columnValue==null || columnValue.equals("")){
                        columnGenerated = new DoubleColumn();
                    }else {
                        columnGenerated = new DoubleColumn(String.valueOf(columnValue).trim());
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format(
                            "字段[%s]类型转换错误, 无法将[%s] 转换为[%s]",name, columnValue,
                            "DECIMAL"));
                }
                break;
            case INT:
            case INTEGER:
                try {
                    if (columnValue==null || columnValue.equals("")){
                        columnGenerated = new LongColumn();
                    }else {
                        columnGenerated = new LongColumn(String.valueOf(columnValue));
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format(
                            "字段[%s]类型转换错误, 无法将[%s] 转换为[%s]",name, columnValue,
                            "INT"));
                }
                break;
            case DOUBLE:
            case FLOAT:
                try {
                    if (columnValue==null || columnValue.equals("")){
                        columnGenerated = new DoubleColumn();
                    }else {
                        columnGenerated = new DoubleColumn(String.valueOf(columnValue));
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format(
                            "字段[%s]类型转换错误, 无法将[%s] 转换为[%s]",name,  columnValue,
                            "DOUBLE"));
                }
                break;
            case BOOLEAN:
                try {
                    if (columnValue==null || columnValue.equals("") ){
                        columnGenerated = new BoolColumn();
                    }else {
                        columnGenerated = toBoolColumn(columnValue);
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format(
                            "字段[%s]类型转换错误, 无法将[%s] 转换为[%s]",name, columnValue,
                            "BOOLEAN"));
                }

                break;
            case DATE:
            case DATETIME:
            case TIMESTAMP:
                try {
                    if (columnValue==null || columnValue.equals("")) {
                        Date date = null;
                        columnGenerated = new DateColumn(date);
                    } else {
                        columnGenerated = toDateColumn(type, columnValue);
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format(
                            "字段[%s]类型转换错误, 无法将[%s] 转换为[%s]",name,  columnValue,
                            type.name()));
                }
                break;
            default:
                String errorMessage = String.format(
                        "您配置的列类型暂不支持 : [%s]", columnType);
                throw AggregationException
                        .asException(
                                UnstructuredStorageReaderErrorCode.NOT_SUPPORT_TYPE,
                                errorMessage);
        }
        return columnGenerated;
    }

    private static BoolColumn toBoolColumn(Object columnValue) {
        if (columnValue instanceof Boolean) {
            return new BoolColumn((Boolean) columnValue);
        }
        if (columnValue instanceof Number) {
            int value = ((Number) columnValue).intValue();
            if (value == 0 || value == 1) {
                return new BoolColumn(Boolean.valueOf(value == 1));
            }
        }
        String text = String.valueOf(columnValue).trim();
        if ("1".equals(text)) {
            return new BoolColumn(Boolean.TRUE);
        }
        if ("0".equals(text)) {
            return new BoolColumn(Boolean.FALSE);
        }
        return new BoolColumn(text);
    }

    private static DateColumn toDateColumn(Type type, Object columnValue) {
        Date parsedDate = parseDate(columnValue);
        if (type == Type.DATE) {
            return new DateColumn(new java.sql.Date(parsedDate.getTime()));
        }
        return new DateColumn(parsedDate);
    }

    private static Date parseDate(Object columnValue) {
        if (columnValue instanceof java.sql.Date) {
            return new Date(((java.sql.Date) columnValue).getTime());
        }
        if (columnValue instanceof java.sql.Timestamp) {
            return new Date(((java.sql.Timestamp) columnValue).getTime());
        }
        if (columnValue instanceof Date) {
            return (Date) columnValue;
        }
        if (columnValue instanceof Number) {
            return new Date(((Number) columnValue).longValue());
        }
        if (columnValue instanceof LocalDate) {
            return java.sql.Date.valueOf((LocalDate) columnValue);
        }
        if (columnValue instanceof LocalDateTime) {
            return Date.from(((LocalDateTime) columnValue).atZone(DEFAULT_ZONE_ID).toInstant());
        }
        String text = String.valueOf(columnValue).trim();
        if (text.isEmpty()) {
            return null;
        }
        if (text.matches("^-?\\d+$")) {
            return new Date(Long.parseLong(text));
        }
        LocalDate localDate = tryParseLocalDate(text);
        if (localDate != null) {
            return java.sql.Date.valueOf(localDate);
        }
        LocalDateTime localDateTime = tryParseLocalDateTime(text);
        if (localDateTime != null) {
            return Date.from(localDateTime.atZone(DEFAULT_ZONE_ID).toInstant());
        }
        try {
            return Date.from(OffsetDateTime.parse(text).toInstant());
        } catch (DateTimeParseException ignored) {
        }
        try {
            return Date.from(Instant.parse(text));
        } catch (DateTimeParseException ignored) {
        }
        return new StringColumn(text).asDate();
    }

    private static LocalDate tryParseLocalDate(String text) {
        try {
            return LocalDate.parse(text, DateTimeFormatter.ISO_LOCAL_DATE);
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }

    private static LocalDateTime tryParseLocalDateTime(String text) {
        String normalized = text.indexOf(' ') > 0 ? text.replace(' ', 'T') : text;
        try {
            return LocalDateTime.parse(normalized, FLEXIBLE_LOCAL_DATE_TIME);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return LocalDateTime.parse(normalized, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        } catch (DateTimeParseException ignored) {
        }
        try {
            return LocalDateTime.parse(normalized, LOCAL_DATE_TIME_WITHOUT_SECONDS);
        } catch (DateTimeParseException ignored) {
            return null;
        }
    }

    private static boolean isDecimalText(Object value) {
        if (value == null) {
            return false;
        }
        String text = String.valueOf(value).trim();
        return text.contains(".") || text.contains("e") || text.contains("E");
    }

    private enum Type {
        STRING, LONG,INT, BOOLEAN, DOUBLE, DECIMAL, DATE,DATETIME,TIMESTAMP,INTEGER,TEXT,NUMBER,FLOAT ;
    }
}
