package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import com.jdragon.aggregation.transformer.util.ValidateUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;


/**
 *去除字符串前后空格
 */
public class TrimSpacesStr extends Transformer {
   int columnIndex;

   public TrimSpacesStr() {
      super.setTransformerName("trim_spaces_str");
   }

   @Override
   public Record evaluate(Record record, Object... paras) {
      try {
         columnIndex = (Integer) paras[0];
         Record newRecord = trimTransformOne(record);
         return newRecord;
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
      }
   }

   /**
    * 去除全部字段的空格
    *
    * @param record
    * @return
    */
   public Record trimTransform(Record record) {
      int columnNumber = record.getColumnNumber();
      for (int i = 0; i < columnNumber; i++) {
         Column column = record.getColumn(i);
         try {
            String oriValue = column.asString();
            if (StringUtils.isBlank(oriValue)) {
               record.setColumn(i, new StringColumn(null));
            } else {
               String newValue = oriValue.trim();
               columnIndex = i;
               record = ValidateUtil.validate(column, newValue, record, columnIndex);
            }
         } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
         }
      }
      return record;
   }

   /**
    * 去除某个字段的空格
    *
    * @param record
    * @return
    */
   public Record trimTransformOne(Record record) {
      Column column = record.getColumn(columnIndex);
      try {
         String oriValue = column.asString();
         if (StringUtils.isBlank(oriValue)) {
            return record;
         } else {
            String newValue = oriValue.trim();
            record = ValidateUtil.validate(column, newValue, record, columnIndex);
         }
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
      }
      return record;
   }


}
