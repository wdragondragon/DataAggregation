package com.jdragon.aggregation.core.transformer.internal;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.core.transformer.TransformerErrorCode;

import java.util.Arrays;

/**
 * no comments.
 * Created by liqiang on 16/3/4.
 */
public class SubstrTransformer extends Transformer {
    public SubstrTransformer() {
        setTransformerName("dx_substr");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {

        int columnIndex;
        int startIndex;
        int length;

        try {
            if (paras.length != 3) {
                throw new RuntimeException("dx_substr paras must be 3");
            }

            columnIndex = (Integer) paras[0];
            startIndex = Integer.parseInt((String) paras[1]);
            length = Integer.parseInt((String) paras[2]);

        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras) + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);

        try {
            String oriValue = column.asString();
            //如果字段为空，跳过subStr处理
            if (oriValue == null) {
                return record;
            }
            String newValue;
            if (startIndex > oriValue.length()) {
                throw new RuntimeException(String.format("dx_substr startIndex(%s) out of range(%s)", startIndex, oriValue.length()));
            }
            if (startIndex + length >= oriValue.length()) {
                newValue = oriValue.substring(startIndex);
            } else {
                newValue = oriValue.substring(startIndex, startIndex + length);
            }

            record.setColumn(columnIndex, new StringColumn(newValue));

        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
