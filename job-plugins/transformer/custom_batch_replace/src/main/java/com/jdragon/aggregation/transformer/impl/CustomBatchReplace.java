package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.transformer.TransformerErrorCode;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CustomBatchReplace extends Transformer {
    @Override
    public Record evaluate(Record record, Object... paras) {
        int columnIndex;
        String para;
        try {
            if (paras.length != 2) {
                throw new RuntimeException("bm_custom_batch_replace paras must be 2");
            }

            columnIndex = (Integer) paras[0];
            para = (String) paras[1];
        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras) + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);

        try {
            String oriValue = column.asString();
            Map<String, String> dict = new HashMap<>();
            String[] keyValues = para.split(";");
            for (String keyValue : keyValues) {
                String[] split = keyValue.split(":");
                dict.put(split[0], split[1]);
            }
            //如果字段为空，跳过replace处理
            if (StringUtils.isBlank(oriValue)) {
                record.setColumn(columnIndex, new StringColumn(null));
            } else {
                String target = dict.get(oriValue.trim());
                record.setColumn(columnIndex, new StringColumn(target));
            }
        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }
}
