package com.jdragon.aggregation.commons.element;

import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.exception.CommonErrorCode;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 */
public class BoolColumn extends Column {

    public BoolColumn(Boolean bool) {
        super(bool, Type.BOOL, 1);
    }

    public BoolColumn(final String data) {
        this(true);
        this.validate(data);
        if (null == data) {
            this.setRawData(null);
            this.setByteSize(0);
        } else {
            this.setRawData(Boolean.valueOf(data));
            this.setByteSize(1);
        }
        return;
    }

    public BoolColumn() {
        super(null, Type.BOOL, 1);
    }

    @Override
    public Boolean asBoolean() {
        if (null == super.getRawData()) {
            return null;
        }

        return (Boolean) super.getRawData();
    }

    @Override
    public Long asLong() {
        if (null == this.getRawData()) {
            return null;
        }

        return this.asBoolean() ? 1L : 0L;
    }

    @Override
    public Double asDouble() {
        if (null == this.getRawData()) {
            return null;
        }

        return this.asBoolean() ? 1.0d : 0.0d;
    }

    @Override
    public String asString() {
        if (null == super.getRawData()) {
            return null;
        }

        return this.asBoolean() ? "true" : "false";
    }

    @Override
    public BigInteger asBigInteger() {
        if (null == this.getRawData()) {
            return null;
        }

        return BigInteger.valueOf(this.asLong());
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (null == this.getRawData()) {
            return null;
        }

        return BigDecimal.valueOf(this.asLong());
    }

    @Override
    public Date asDate() {
        throw AggregationException.asException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Bool类型不能转为Date .");
    }

    @Override
    public byte[] asBytes() {
        throw AggregationException.asException(
                CommonErrorCode.CONVERT_NOT_SUPPORT, "Boolean类型不能转为Bytes .");
    }

    private void validate(final String data) {
        if (null == data) {
            return;
        }

        if ("true".equalsIgnoreCase(data) || "false".equalsIgnoreCase(data)) {
            return;
        }

        throw AggregationException.asException(
                CommonErrorCode.CONVERT_NOT_SUPPORT,
                String.format("String[%s]不能转为Bool .", data));
    }

    @Override
    public Column clone() throws CloneNotSupportedException {
        return new BoolColumn(this.asBoolean());
    }
}
