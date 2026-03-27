package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;

/**
 * @author jdragon
 * @date 2026/3/27
 * 数字过滤
 */
@Slf4j
public class NumberOperationFilter extends Transformer {
   public NumberOperationFilter() {
      setTransformerName("number_operation_filter");
   }

   @Override
   public Record evaluate(Record record, Object... paras) {
      int columnIndex;
      String operation;
      double value;
      try {
         if (paras.length != 3) {
            throw new RuntimeException("number_operation_filter paras must be 3");
         }

         columnIndex = (Integer) paras[0];
         operation =  paras[1].toString();
         value = Double.parseDouble(paras[2].toString());
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
      }

      Column column = record.getColumn(columnIndex);

      try {
         Double oriValue = column.asDouble();
         Column.Type type = column.getType();
         if (type == Column.Type.LONG || type == Column.Type.INT || type == Column.Type.DOUBLE) {
            if (oriValue == null) {
               return record;
            } else {
               if (operation != null) {
                  switch (operation) {
                     case ">":
                        if (oriValue > value) {
                           return null;
                        } else {
                           return record;
                        }
                     case "<":
                        if (oriValue < value) {
                           return null;
                        } else {
                           return record;
                        }
                     case "=":
                        if (oriValue == value) {
                           return null;
                        } else {
                           return record;
                        }
                     case "!=":
                        if (oriValue != value) {
                           return null;
                        } else {
                           return record;
                        }
                     case ">=":
                        if (oriValue > value || oriValue == value) {
                           return null;
                        } else {
                           return record;
                        }
                     case "<=":
                        if (oriValue < value || oriValue == value) {
                           return null;
                        } else {
                           return record;
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
