package com.jdragon.aggregation.commons.element;

import com.alibaba.fastjson.JSON;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

public abstract class Column implements Cloneable, Comparable<Column> {

    private Type type;

    private Object rawData;

    private int byteSize;


    public Column(final Object object, final Type type, int byteSize) {
        this.rawData = object;
        this.type = type;
        this.byteSize = byteSize;
    }

    public Object getRawData() {
        return this.rawData;
    }

    public Type getType() {
        return this.type;
    }

    public int getByteSize() {
        return this.byteSize;
    }

    protected void setType(Type type) {
        this.type = type;
    }

    protected void setRawData(Object rawData) {
        this.rawData = rawData;
    }

    protected void setByteSize(int byteSize) {
        this.byteSize = byteSize;
    }

    public abstract Long asLong();

    public abstract Double asDouble();

    public abstract String asString();

    public abstract Date asDate();

    public abstract byte[] asBytes();

    public abstract Boolean asBoolean();

    public abstract BigDecimal asBigDecimal();

    public abstract BigInteger asBigInteger();

    @Override
    public Column clone() throws CloneNotSupportedException {
        return (Column) super.clone();
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    public enum Type {
        BAD, NULL, INT, LONG, DOUBLE, STRING, BOOL, DATE, BYTES, OBJECT
    }

    @Override
    public int compareTo(Column column) {
        if (type.equals(column.getType())) {
            switch (type) {
                case INT:
                case LONG:
                case DOUBLE:
                    return this.asDouble().compareTo(column.asDouble());
                case DATE:
                    return this.asDate().compareTo(column.asDate());
                case STRING:
                    return this.asString().compareTo(column.asString());
            }
        }
        throw new RuntimeException("不支持的类型对比");
    }
}
