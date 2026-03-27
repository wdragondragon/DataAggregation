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
public class StringOperationFilter extends Transformer {
   public StringOperationFilter() {
      setTransformerName("string_operation_filter");
   }

   @Override
   public Record evaluate(Record record, Object... paras) {
      int columnIndex;
      String saveOrDelete;
      String operation;
      String value;
      try {
         if (paras.length != 4) {
            throw new RuntimeException("string_operation_filter paras must be 4");
         }

         columnIndex = (Integer) paras[0];
         saveOrDelete = paras[1].toString();
         operation = paras[2].toString();
         value = paras[3].toString();
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
      }

      Column column = record.getColumn(columnIndex);

      try {
         String oriValue = column.asString();
         Column.Type type = column.getType();
         if (type == Column.Type.STRING || type == Column.Type.DOUBLE || type == Column.Type.LONG || type == Column.Type.INT) {
            if (StringUtils.isNotBlank(value)) {
               String[] words = value.split(",");
               if ("delete".equals(saveOrDelete)) {
                  if (StringUtils.isBlank(oriValue)){
                     return record;
                  } else {
                     if (StringUtils.isNotBlank(operation)) {
                        switch (operation) {
                           case "Yes":
                              for (String word : words) {
                                 if (oriValue.equals(word)) {
                                    return null;
                                 }
                              }
                              return record;
                           case "No":
                              for (String word : words) {
                                 if (oriValue.contains(word)) {
                                    return null;
                                 }
                              }
                              return record;
                        }
                     }
                  }
               } else if ("save".equals(saveOrDelete)) {
                  if (StringUtils.isBlank(oriValue)){
                     return null;
                  } else {
                     if (StringUtils.isNotBlank(operation)) {
                        switch (operation) {
                           case "Yes":
                              for (String word : words) {
                                 if (oriValue.equals(word)) {
                                    return record;
                                 }
                              }
                              return null;
                           case "No":
                              for (String word : words) {
                                 if (oriValue.contains(word)) {
                                    return record;
                                 }
                              }
                              return null;
                        }
                     }
                  }
               }
            }
         }
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
      }
      return record;
   }
}
