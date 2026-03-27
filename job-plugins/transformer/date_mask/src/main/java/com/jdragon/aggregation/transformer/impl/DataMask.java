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
 * <p>
 * 覆盖脱敏
 */
public class DataMask extends Transformer {
    public DataMask() {
       super.setTransformerName("date_mask");
    }

   @Override
   public Record evaluate(Record record, Object... paras) {
      int columnIndex;
      String hideOrShow;
      String beforeNum;
      String afterNum;
      String centerNum;
      try {
         if (paras.length != 5) {
            throw new RuntimeException("bm_date_mask paras must be 5");
         }
         columnIndex = (Integer) paras[0];
         hideOrShow = paras[1].toString();
         beforeNum = paras[2].toString();
         centerNum = paras[3].toString();
         afterNum = paras[4].toString();
      } catch (Exception e) {
          throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
      }

      Column column = record.getColumn(columnIndex);
      try {
         String oriValue = column.asString();
         //如果字段为空，跳过replace处理
         if (StringUtils.isBlank(oriValue) || StringUtils.isBlank(hideOrShow)) {
            return record;
         } else {
            if ("hide".equals(hideOrShow)) {
               if (StringUtils.isNotBlank(beforeNum) && Integer.valueOf(beforeNum) > 0) {
                  if (oriValue.length() > Integer.valueOf(beforeNum)) {
                     //oriValue = new String(new char[Integer.valueOf(beforeNum)]).replace("\0", "*") + oriValue.substring(Integer.valueOf(beforeNum));
                     oriValue = StringUtils.leftPad(oriValue.substring(Integer.valueOf(beforeNum)), oriValue.length(), "*");
                  } else {
                     //oriValue = new String(new char[oriValue.length()]).replace("\0", "*");
                     oriValue = StringUtils.leftPad("", oriValue.length(), "*");
                  }
               }
               if (StringUtils.isNotBlank(centerNum) && Integer.valueOf(centerNum) > 0) {
                  int strLen = oriValue.length();
                  if (strLen > Integer.valueOf(centerNum)) {
                     int center = Integer.valueOf(centerNum);
                     int num = strLen / 2 - center / 2;
                     oriValue = oriValue.substring(0, num) + StringUtils.center("", center, "*") + oriValue.substring(num + center);
                  } else {
                     oriValue = StringUtils.center("", strLen, "*");
                  }
               }
               if (StringUtils.isNotBlank(afterNum) && Integer.valueOf(afterNum) > 0) {
                  if (oriValue.length() > Integer.valueOf(afterNum)) {
                     //oriValue =oriValue.substring(0, oriValue.length() - Integer.valueOf(beforeNum)) + new String(new char[Integer.valueOf(beforeNum)]).replace("\0", "*");
                     oriValue = StringUtils.rightPad(oriValue.substring(0, oriValue.length() - Integer.valueOf(afterNum)), oriValue.length(), "*");
                  } else {
                     //oriValue = new String(new char[oriValue.length()]).replace("\0", "*");
                     oriValue = StringUtils.rightPad("", oriValue.length(), "*");
                  }
               }
            } else if ("show".equals(hideOrShow)) {
               //前中后都不为空
               if (StringUtils.isNotBlank(beforeNum) && Integer.valueOf(beforeNum) > 0 && StringUtils.isNotBlank(centerNum) && Integer.valueOf(centerNum) > 0 && StringUtils.isNotBlank(afterNum) && Integer.valueOf(afterNum) > 0) {
                  if (oriValue.length() > Integer.valueOf(beforeNum) + Integer.valueOf(centerNum) + Integer.valueOf(afterNum)) {
                     int strLen = oriValue.length();
                     int before = Integer.valueOf(beforeNum);
                     int center = Integer.valueOf(centerNum);
                     int after = Integer.valueOf(afterNum);
                     int num = strLen / 2 - center / 2;

                     oriValue = oriValue.substring(0, before) + StringUtils.center(oriValue.substring(num, num + center), strLen - after - before, "*") + oriValue.substring(strLen - after);
                  }
               }//前中不为空
               else if (StringUtils.isNotBlank(beforeNum) && Integer.valueOf(beforeNum) > 0 && StringUtils.isNotBlank(centerNum) && Integer.valueOf(centerNum) > 0 && (StringUtils.isBlank(afterNum) || (StringUtils.isNotBlank(afterNum) && Integer.valueOf(afterNum) <= 0))) {
                  if (oriValue.length() > Integer.valueOf(beforeNum) + Integer.valueOf(centerNum)) {
                     int strLen = oriValue.length();
                     int before = Integer.valueOf(beforeNum);
                     int center = Integer.valueOf(centerNum);
                     int num = strLen / 2 - center / 2;
                     oriValue = StringUtils.rightPad(StringUtils.rightPad(oriValue.substring(0, before), num, "*") + oriValue.substring(num, num + center), strLen, "*");
                  }
               }//中后不为空
               else if ((StringUtils.isBlank(beforeNum) || (StringUtils.isNotBlank(beforeNum) && Integer.valueOf(beforeNum) <= 0)) && StringUtils.isNotBlank(centerNum) && Integer.valueOf(centerNum) > 0 && StringUtils.isNotBlank(afterNum) && Integer.valueOf(afterNum) > 0) {
                  if (oriValue.length() > Integer.valueOf(afterNum) + Integer.valueOf(centerNum)) {
                     int strLen = oriValue.length();
                     int after = Integer.valueOf(afterNum);
                     int center = Integer.valueOf(centerNum);
                     int num = strLen / 2 - center / 2;
                     oriValue = StringUtils.rightPad("", num, "*") + oriValue.substring(num, num + center) + StringUtils.leftPad(oriValue.substring(strLen - after), strLen - num - center, "*");
                  }
               }//前后不为空
               else if (StringUtils.isNotBlank(beforeNum) && Integer.valueOf(beforeNum) > 0 && (StringUtils.isBlank(centerNum) || (StringUtils.isNotBlank(centerNum) && Integer.valueOf(centerNum) <= 0)) && StringUtils.isNotBlank(afterNum) && Integer.valueOf(afterNum) > 0) {
                  if (oriValue.length() > Integer.valueOf(beforeNum) + Integer.valueOf(afterNum)) {
                     int strLen = oriValue.length();
                     int before = Integer.valueOf(beforeNum);
                     int after = Integer.valueOf(afterNum);

                     oriValue = StringUtils.rightPad(oriValue.substring(0, before), strLen - after, "*") + oriValue.substring(strLen - after);
                  }
               }//前不为空
               else if (StringUtils.isNotBlank(beforeNum) && Integer.valueOf(beforeNum) > 0 && (StringUtils.isBlank(centerNum) || (StringUtils.isNotBlank(centerNum) && Integer.valueOf(centerNum) <= 0)) && (StringUtils.isBlank(afterNum) || (StringUtils.isNotBlank(afterNum) && Integer.valueOf(afterNum) <= 0))) {
                  if (oriValue.length() > Integer.valueOf(beforeNum)) {
                     oriValue = StringUtils.rightPad(oriValue.substring(0, Integer.valueOf(beforeNum)), oriValue.length(), "*");
                  }
               }//中不为空
               else if ((StringUtils.isBlank(beforeNum) || (StringUtils.isNotBlank(beforeNum) && Integer.valueOf(beforeNum) <= 0)) && StringUtils.isNotBlank(centerNum) && Integer.valueOf(centerNum) > 0 && (StringUtils.isBlank(afterNum) || (StringUtils.isNotBlank(afterNum) && Integer.valueOf(afterNum) <= 0))) {
                  int strLen = oriValue.length();
                  if (strLen > Integer.valueOf(centerNum)) {
                     int center = Integer.valueOf(centerNum);
                     int num = strLen / 2 - center / 2;
                     oriValue = StringUtils.rightPad(StringUtils.center("", num, "*") + oriValue.substring(num, num + center), strLen, "*");
                  }
               }//后不为空
               else if ((StringUtils.isBlank(beforeNum) || (StringUtils.isNotBlank(beforeNum) && Integer.valueOf(beforeNum) <= 0)) && (StringUtils.isBlank(centerNum) || (StringUtils.isNotBlank(centerNum) && Integer.valueOf(centerNum) <= 0)) && StringUtils.isNotBlank(afterNum) && Integer.valueOf(afterNum) > 0) {
                  if (oriValue.length() > Integer.valueOf(afterNum)) {
                     oriValue = StringUtils.leftPad(oriValue.substring(oriValue.length() - Integer.valueOf(afterNum)), oriValue.length(), "*");
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
