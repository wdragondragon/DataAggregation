package com.jdragon.aggregation.commons.element;

import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.exception.CommonErrorCode;
import org.apache.commons.lang3.ArrayUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

/**
 * Created by jingxing on 14-8-24.
 */
public class BytesColumn extends Column {

	public BytesColumn() {
		this(null);
	}

	public BytesColumn(byte[] bytes) {
		super(ArrayUtils.clone(bytes), Type.BYTES, null == bytes ? 0
				: bytes.length);
	}

	@Override
	public byte[] asBytes() {
		if (null == this.getRawData()) {
			return null;
		}

		return (byte[]) this.getRawData();
	}

	@Override
	public String asString() {
		if (null == this.getRawData()) {
			return null;
		}

		try {
			return ColumnCast.bytes2String(this);
		} catch (Exception e) {
			throw AggregationException.asException(
					CommonErrorCode.CONVERT_NOT_SUPPORT,
					String.format("Bytes[%s]不能转为String .", this.toString()));
		}
	}

	@Override
	public Long asLong() {
		throw AggregationException.asException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Long .");
	}

	@Override
	public BigDecimal asBigDecimal() {
		throw AggregationException.asException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为BigDecimal .");
	}

	@Override
	public BigInteger asBigInteger() {
		throw AggregationException.asException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为BigInteger .");
	}

	@Override
	public Double asDouble() {
		throw AggregationException.asException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Long .");
	}

	@Override
	public Date asDate() {
		throw AggregationException.asException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Date .");
	}

	@Override
	public Boolean asBoolean() {
		throw AggregationException.asException(
				CommonErrorCode.CONVERT_NOT_SUPPORT, "Bytes类型不能转为Boolean .");
	}

	@Override
	public Column clone() throws CloneNotSupportedException {
		return new BytesColumn(asBytes());
	}
}
