package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
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
public class NullValueFilter extends Transformer {
   public NullValueFilter() {
      setTransformerName("null_value_filter");
   }

   @Override
   public Record evaluate(Record record, Object... paras) {
      int columnIndex;
      try {
         if (paras.length != 1) {
            throw new RuntimeException("null_value_filter paras must be 1");
         }

         columnIndex = (Integer) paras[0];
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
      }

      Column column = record.getColumn(columnIndex);

      try {
         String oriValue = column.asString();

         if (StringUtils.isBlank(oriValue)) {
            return null;
         }
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
      }
      return record;
   }
}
