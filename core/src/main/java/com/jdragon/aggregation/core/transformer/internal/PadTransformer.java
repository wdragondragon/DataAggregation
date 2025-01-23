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
public class PadTransformer extends Transformer {
    public PadTransformer() {
        setTransformerName("dx_pad");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {

        int columnIndex;
        String padType;
        int length;
        String padString;

        try {
            if (paras.length != 4) {
                throw new RuntimeException("dx_pad paras must be 4");
            }

            columnIndex = (Integer) paras[0];
            padType = (String) paras[1];
            length = Integer.parseInt((String) paras[2]);
            padString = (String) paras[3];
        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }

        Column column = record.getColumn(columnIndex);

        try {
            String oriValue = column.asString();

            //如果字段为空，作为空字符串处理
            if (oriValue == null) {
                oriValue = "";
            }
            String newValue;
            if (!padType.equalsIgnoreCase("r") && !padType.equalsIgnoreCase("l")) {
                throw new RuntimeException(String.format("dx_pad first para(%s) support l or r", padType));
            }
            if (length <= oriValue.length()) {
                newValue = oriValue.substring(0, length);
            } else {

                newValue = doPad(padType, oriValue, length, padString);
            }

            record.setColumn(columnIndex, new StringColumn(newValue));

        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
        }
        return record;
    }

    private String doPad(String padType, String oriValue, int length, String padString) {

        StringBuilder finalPad = new StringBuilder();
        int NeedLength = length - oriValue.length();
        while (NeedLength > 0) {

            if (NeedLength >= padString.length()) {
                finalPad.append(padString);
                NeedLength -= padString.length();
            } else {
                finalPad.append(padString, 0, NeedLength);
                NeedLength = 0;
            }
        }

        if (padType.equalsIgnoreCase("l")) {
            return finalPad + oriValue;
        } else {
            return oriValue + finalPad;
        }
    }

}
