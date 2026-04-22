package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Arrays;

/**
 * @author jdragon
 * @date 2026/3/27
 * 保留小数点后n位
 */
public class NumberCut extends Transformer {
   public NumberCut() {
      super.setTransformerName("number_cut");
   }

   @Override
   public Record evaluate(Record record, Object... paras) {
      int columnIndex;
      String num;
      try {
         if (paras.length != 2) {
            throw new RuntimeException("number_cut paras must be 2");
         }
         columnIndex = (Integer) paras[0];
         num = paras[1].toString();
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
      }

      Column column = record.getColumn(columnIndex);
      try {
         String oriValue = column.asString();
         Column.Type type = column.getType();
         if (type == Column.Type.STRING || type == Column.Type.DOUBLE || type == Column.Type.LONG || type == Column.Type.INT) {
            if (StringUtils.isBlank(oriValue)) {
               return record;
            } else {
               if (StringUtils.isNotBlank(num) && Integer.valueOf(num) >= 0) {
                  if (Integer.valueOf(num) == 0) {
                     oriValue = new DecimalFormat("0").format(Double.valueOf(oriValue));
                  } else {
                     String formatStr = "0.";
                     for (int i = 0; i < Integer.valueOf(num); i++) {
                        formatStr = formatStr + "0";
                     }
                     DecimalFormat decimalFormat = new DecimalFormat(formatStr);
                     decimalFormat.setRoundingMode(RoundingMode.FLOOR);
                     oriValue = decimalFormat.format(Double.valueOf(oriValue));
                  }
               }
               record.setColumn(columnIndex, new StringColumn(oriValue));
            }
         } else {
            return record;
         }
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
      }
      return record;
   }
}
