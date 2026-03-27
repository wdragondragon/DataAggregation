package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * @author jdragon
 * @date 2026/3/27
 * 日期过滤
 */
@Slf4j
public class DateOperationFilter extends Transformer {
    public DateOperationFilter() {
      setTransformerName("date_operation_filter");
   }

   @Override
   public Record evaluate(Record record, Object... paras) {
      int columnIndex;
      String saveOrDelete;
      String valueDf;
      String startValue = null;
      String endValue = null;
      try {

         columnIndex = (Integer) paras[0];
         saveOrDelete = paras[1].toString();
         valueDf = paras[2].toString();
         startValue = paras[3].toString();
         endValue = paras[4].toString();

      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
      }

      Column column = record.getColumn(columnIndex);

      try {
         Date oriValue = column.asDate();
         Column.Type type = column.getType();
         if (type == Column.Type.DATE) {
            if (valueDf != null) {
               SimpleDateFormat valueFormat = new SimpleDateFormat(valueDf);
               if ("delete".equals(saveOrDelete)) {
                  if (oriValue == null) {
                     return record;
                  } else {
                     long dataTime = oriValue.getTime();
                     if (StringUtils.isNotBlank(startValue) && StringUtils.isNotBlank(endValue)) {
                        long startTime = valueFormat.parse(startValue).getTime();
                        long endTime = valueFormat.parse(endValue).getTime();

                        if (dataTime > startTime && dataTime < endTime) {
                           return null;
                        } else {
                           return record;
                        }
                     } else if (StringUtils.isBlank(startValue) && StringUtils.isNotBlank(endValue)) {
                        long endTime = valueFormat.parse(endValue).getTime();
                        if (dataTime < endTime) {
                           return null;
                        } else {
                           return record;
                        }
                     } else if (StringUtils.isNotBlank(startValue) && StringUtils.isBlank(endValue)) {
                        long startTime = valueFormat.parse(startValue).getTime();
                        if (dataTime > startTime) {
                           return null;
                        } else {
                           return record;
                        }
                     }
                  }
               } else if ("save".equals(saveOrDelete)) {
                  if (oriValue == null) {
                     return null;
                  } else {
                     long dataTime = oriValue.getTime();
                     if (StringUtils.isNotBlank(startValue) && StringUtils.isNotBlank(endValue)) {
                        long startTime = valueFormat.parse(startValue).getTime();
                        long endTime = valueFormat.parse(endValue).getTime();
                        if (dataTime > startTime && dataTime < endTime) {
                           return record;
                        } else {
                           return null;
                        }
                     } else if (StringUtils.isBlank(startValue) && StringUtils.isNotBlank(endValue)) {
                        long endTime = valueFormat.parse(endValue).getTime();
                        if (dataTime < endTime) {
                           return record;
                        } else {
                           return null;
                        }
                     } else if (StringUtils.isNotBlank(startValue) && StringUtils.isBlank(endValue)) {
                        long startTime = valueFormat.parse(startValue).getTime();
                        if (dataTime > startTime) {
                           return record;
                        } else {
                           return null;
                        }
                     }
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
