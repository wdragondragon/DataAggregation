package com.jdragon.aggregation.unstructuredstorage;

import com.jdragon.aggregation.commons.element.*;
import com.jdragon.aggregation.commons.exception.AggregationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            case TIMESTAMP:
                try {
                    if (columnValue==null || columnValue.equals("")){
                        columnGenerated = new LongColumn();
                    }else {
                        columnGenerated = new LongColumn(String.valueOf(columnValue));
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format(
                            "字段[%s]类型转换错误, 无法将[%s] 转换为[%s]",name, columnValue,
                            "LONG"));
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
                        columnGenerated = new BoolColumn(String.valueOf(columnValue));
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format(
                            "字段[%s]类型转换错误, 无法将[%s] 转换为[%s]",name, columnValue,
                            "BOOLEAN"));
                }

                break;
            case DATE:
            case DATETIME:
                try {
                    if (columnValue==null || columnValue.equals("")) {
                        Date date = null;
                        columnGenerated = new DateColumn(date);
                    } else {
                        // 框架尝试转换
                        columnGenerated = new DateColumn(
                                new StringColumn(String.valueOf(columnValue))
                                        .asDate());
                    }
                } catch (Exception e) {
                    throw new IllegalArgumentException(String.format(
                            "字段[%s]类型转换错误, 无法将[%s] 转换为[%s]",name,  columnValue,
                            "DATE"));
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

    private enum Type {
        STRING, LONG,INT, BOOLEAN, DOUBLE, DATE,DATETIME,TIMESTAMP,INTEGER,TEXT,NUMBER,FLOAT ;
    }
}
