package com.jdragon.aggregation.commons.element;

import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.exception.CommonErrorCode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * @Author JDragon
 * @Date 2022.06.27 下午 3:19
 * @Email 1061917196@qq.com
 * @Des:
 */
public class ObjectColumn extends Column {

    public ObjectColumn(Object object) {
        super(object, Type.OBJECT, 0);
    }

    @Override
    public Long asLong() {
        if (null == this.getRawData()) {
            return null;
        }
        return Long.valueOf(asString());
    }

    @Override
    public Double asDouble() {
        if (null == this.getRawData()) {
            return null;
        }
        return Double.valueOf(asString());
    }

    @Override
    public String asString() {
        if (null == this.getRawData()) {
            return null;
        }
        return String.valueOf(getRawData());
    }

    @Override
    public Date asDate() {
        if (null == this.getRawData()) {
            return null;
        }
        return new Date(this.asLong());
    }

    @Override
    public byte[] asBytes() {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(getRawData());
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("as Bytes fail");
        }
    }

    @Override
    public Boolean asBoolean() {
        if (null == this.getRawData()) {
            return null;
        }
        return Boolean.parseBoolean(asString());
    }

    @Override
    public BigDecimal asBigDecimal() {
        if (null == this.getRawData()) {
            return null;
        }

        this.validateDoubleSpecific((String) this.getRawData());

        try {
            return new BigDecimal(this.asString());
        } catch (Exception e) {
            throw AggregationException.asException(
                    CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                            "String [\"%s\"] 不能转为BigDecimal .", this.asString()));
        }
    }

    @Override
    public BigInteger asBigInteger() {
        if (null == this.getRawData()) {
            return null;
        }

        this.validateDoubleSpecific((String) this.getRawData());

        try {
            return this.asBigDecimal().toBigInteger();
        } catch (Exception e) {
            throw AggregationException.asException(
                    CommonErrorCode.CONVERT_NOT_SUPPORT, String.format(
                            "String[\"%s\"]不能转为BigInteger .", this.asString()));
        }
    }


    private void validateDoubleSpecific(final String data) {
        if ("NaN".equals(data) || "Infinity".equals(data)
                || "-Infinity".equals(data)) {
            throw AggregationException.asException(
                    CommonErrorCode.CONVERT_NOT_SUPPORT,
                    String.format("String[\"%s\"]属于Double特殊类型，不能转为其他类型 .", data));
        }
    }
}
