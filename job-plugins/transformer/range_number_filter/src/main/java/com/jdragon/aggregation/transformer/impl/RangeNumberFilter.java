package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * @author jdragon
 * @date 2026/3/27
 * 值区间过滤
 */
public class RangeNumberFilter extends Transformer {
   public RangeNumberFilter() {
       setTransformerName("range_number_filter");
   }

   @Override
   public Record evaluate(Record record, Object... paras) {
      int columnIndex;
      String saveOrDelete;
      double startValue;
      double endValue;
      try {

         columnIndex = (Integer) paras[0];
         saveOrDelete = paras[1].toString();
         startValue = Double.parseDouble(paras[2].toString());
         endValue = Double.parseDouble(paras[3].toString());

      } catch (Exception e) {
          throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
      }

      Column column = record.getColumn(columnIndex);

      try {
         Column.Type type = column.getType();
         if (type == Column.Type.DOUBLE || type == Column.Type.LONG || type == Column.Type.INT) {
            String dataStr = column.asString();

            if ("delete".equals(saveOrDelete)) {
               if (StringUtils.isBlank(dataStr)) {
                  return record;
               } else {
                  Double oriValue = Double.valueOf(dataStr);
                  if (oriValue > startValue && oriValue < endValue) {
                     return null;
                  } else if(oriValue == startValue || oriValue == endValue){
                     return null;
                  }else {
                     return record;
                  }
               }
            } else if ("save".equals(saveOrDelete)) {
               if (StringUtils.isBlank(dataStr)) {
                  return null;
               } else {
                  Double oriValue = Double.valueOf(dataStr);
                  if (oriValue > startValue && oriValue < endValue) {
                     return record;
                  }else if(oriValue == startValue || oriValue == endValue){
                     return record;
                  } else {
                     return null;
                  }
               }
            }
         }
      } catch (
              Exception e) {
          throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
      }
      return record;
   }
}
