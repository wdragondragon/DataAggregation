package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.util.ValidateUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;

/**
 * <p>
 * <p>替换字符
 * </p>
 *
 * @author jdragon
 * @since 2026/3/27
 */
public class ReplaceStr extends Transformer {
    int columnIndex;
    List<String> regexList;
    String replaceMent;
    public ReplaceStr() {
        super.setTransformerName("replace_str");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        String regex ;
        try {
            if (paras.length != 3) {
                throw new RuntimeException("replace_str paras must be 3");
            }
            columnIndex = (Integer) paras[0];
            regex = (String) paras[1];
            replaceMent = (String) paras[2];
        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Record newRecord = null;
        try {
            if (StringUtils.isBlank(regex)){
                return record;
            }
            String[] split = regex.split(",");
            regexList = Arrays.asList(split);
            newRecord = replaceTransform(record);
        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),e);
        }
        return newRecord;
    }

    /**
     * 字符替换
     * @param record
     * @return
     */
    public Record replaceTransform(Record record){
        Column column = record.getColumn(columnIndex);
        String oriValue = column.asString();
        if (StringUtils.isBlank(oriValue)){
            return record;
        }
        for (String s : regexList) {
            if (oriValue.contains(s)) {
                String newValue = oriValue.replaceAll(s, replaceMent);
                record = ValidateUtil.validate(column, newValue, record, columnIndex);
            }
        }
        return record;
    }
}
