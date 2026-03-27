package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * <p>
 * <p>MD5加密
 * </p>
 *
 * @author jdragon
 * @since 2026/3/27
 */
public class MD5Str extends Transformer {
   int columnIndex;

   public MD5Str() {
      super.setTransformerName("MD5_str");
   }

   @Override
   public Record evaluate(Record record, Object... paras) {
      int columnIndex;
      String key;
      try {
         if (paras.length != 2) {
            throw new RuntimeException("MD5_str paras must be 2");
         }
         columnIndex = (Integer) paras[0];
         key = paras[1].toString();
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
      }

      Column column = record.getColumn(columnIndex);
      try {
         String oriValue = column.asString();
         if (StringUtils.isBlank(oriValue)) {
            return record;
         } else {
            if (StringUtils.isBlank(key)) {
               oriValue = DigestUtils.md5Hex(oriValue);
            } else {
               oriValue = DigestUtils.md5Hex(oriValue + key);
            }
            record.setColumn(columnIndex, new StringColumn(oriValue));
         }
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
      }
      return record;
   }
}
