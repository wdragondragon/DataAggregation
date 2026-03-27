package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * @author jdragon
 * @date 2026/3/27
 * 枚举值过滤
 */
@Slf4j
public class NullValueReplace extends Transformer {
    public NullValueReplace() {
        setTransformerName("null_value_replace");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        int columnIndex;

        String replaceValue;
        try {
            if (paras.length != 2) {
                throw new RuntimeException("null_value_replace paras must be 2");
            }

            columnIndex = (Integer) paras[0];

            replaceValue = (String) paras[1];
        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);

        try {
            String oriValue = column.asString();

            if (StringUtils.isBlank(oriValue)) {
                record.setColumn(columnIndex, new StringColumn(replaceValue));
            }
        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
