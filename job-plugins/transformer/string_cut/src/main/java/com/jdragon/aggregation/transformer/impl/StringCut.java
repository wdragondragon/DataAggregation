package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * @author jdragon
 * @date 2026/3/27
 * 字符截取
 */
public class StringCut extends Transformer {
   public StringCut() {
      super.setTransformerName("string_cut");
   }

   @Override
   public Record evaluate(Record record, Object... paras) {
      int columnIndex;
      String saveOrDelete;
      String beforeNum;
      String afterNum;
      try {
         if (paras.length != 4) {
            throw new RuntimeException("string_cut paras must be 4");
         }
         columnIndex = (Integer) paras[0];
         saveOrDelete = paras[1].toString();
         beforeNum = paras[2].toString();
         afterNum = paras[3].toString();
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
      }

      Column column = record.getColumn(columnIndex);
      try {
         String oriValue = column.asString();
         if (StringUtils.isBlank(oriValue) || StringUtils.isBlank(saveOrDelete)) {
            return record;
         } else {
            if ("delete".equals(saveOrDelete)) {
               //前后都不为空
               if (StringUtils.isNotBlank(beforeNum) && Integer.valueOf(beforeNum) > 0 && StringUtils.isNotBlank(afterNum) && Integer.valueOf(afterNum) > 0) {
                  if (oriValue.length() > Integer.valueOf(beforeNum) + Integer.valueOf(afterNum)) {
                     oriValue = oriValue.substring(Integer.valueOf(beforeNum), oriValue.length() - Integer.valueOf(afterNum));
                  } else {
                     oriValue = "";
                  }
               }//前不为空
               else if (StringUtils.isNotBlank(beforeNum) && Integer.valueOf(beforeNum) > 0 && (StringUtils.isBlank(afterNum) || (StringUtils.isNotBlank(afterNum) && Integer.valueOf(afterNum) <= 0))) {
                  if (oriValue.length() > Integer.valueOf(beforeNum)) {
                     oriValue = oriValue.substring(Integer.valueOf(beforeNum));
                  } else {
                     oriValue = "";
                  }
               }//后不为空
               else if ((StringUtils.isBlank(beforeNum) || StringUtils.isNotBlank(beforeNum) && Integer.valueOf(beforeNum) <= 0) && StringUtils.isNotBlank(afterNum) && Integer.valueOf(afterNum) > 0) {
                  if (oriValue.length() > Integer.valueOf(afterNum)) {
                     oriValue = oriValue.substring(0, oriValue.length() - Integer.valueOf(afterNum));
                  } else {
                     oriValue = "";
                  }
               }
            } else if ("save".equals(saveOrDelete)) {
               //前后都不为空
               if (StringUtils.isNotBlank(beforeNum) && Integer.valueOf(beforeNum) > 0 && StringUtils.isNotBlank(afterNum) && Integer.valueOf(afterNum) > 0) {
                  if (oriValue.length() > Integer.valueOf(beforeNum) + Integer.valueOf(afterNum)) {
                     oriValue = oriValue.substring(0, Integer.valueOf(beforeNum)) + oriValue.substring(oriValue.length() - Integer.valueOf(afterNum));
                  }
               }//前不为空
               else if (StringUtils.isNotBlank(beforeNum) && Integer.valueOf(beforeNum) > 0 && (StringUtils.isBlank(afterNum) || (StringUtils.isNotBlank(afterNum) && Integer.valueOf(afterNum) <= 0))) {
                  if (oriValue.length() > Integer.valueOf(beforeNum)) {
                     oriValue = oriValue.substring(0, Integer.valueOf(beforeNum));
                  }
               }//后不为空
               else if ((StringUtils.isBlank(beforeNum) || StringUtils.isNotBlank(beforeNum) && Integer.valueOf(beforeNum) <= 0) && StringUtils.isNotBlank(afterNum) && Integer.valueOf(afterNum) > 0) {
                  if (oriValue.length() > Integer.valueOf(afterNum)) {
                     oriValue = oriValue.substring(oriValue.length() - Integer.valueOf(afterNum));
                  }
               }
            }
            record.setColumn(columnIndex, new StringColumn(oriValue));
         }
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
      }
      return record;
   }
}
