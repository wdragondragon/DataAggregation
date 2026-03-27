package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

/**
 * @author jdragon
 * @date 2026/3/27
 * 添加默认值
 */
@Slf4j
public class AddDefaultValue extends Transformer {
   public AddDefaultValue() {
      setTransformerName("add_default_value");
   }

   @Override
   public Record evaluate(Record record, Object... paras) {
      int columnIndex;
      String defaultValue;
      try {
         if (paras.length != 2) {
             throw new RuntimeException("add_default_value paras must be 2");
         }

         columnIndex = (Integer) paras[0];
         defaultValue = (String) paras[1];
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
      }

      try {
         record.setColumn(columnIndex, new StringColumn(defaultValue));
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
      }
      return record;
   }

}
