package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author jdragon
 * @date 2026/3/27
 * 字符串日期格式转换
 */
@Slf4j
public class DateTransformer extends Transformer {

   int columnIndex;
   String dateFormatOld;
   String dateFormatNew;

   public DateTransformer() {
      super.setTransformerName("date_transformer");
   }

   @Override
   public Record evaluate(Record record, Object... paras) {
      columnIndex = (Integer) paras[0];
      dateFormatOld = (String) paras[1];
      dateFormatNew = (String) paras[2];

      Record newRecord = dateFormatTimestamp(record);
      return newRecord;
   }

   /**
    * @param record
    * @return
    */
   public Record dateFormatTimestamp(Record record) {
      Column column = record.getColumn(columnIndex);

      Date date = null;
      try {
         String oriValue = column.asString();
         if (StringUtils.isBlank(oriValue)) {
            return record;
         }

         if ("stamp".equals(dateFormatOld)) {
            if (oriValue.length()==10) {
                oriValue = oriValue + "000";
            }
            long lt = new Long(oriValue);
            date = new Date(lt);
         } else {
            SimpleDateFormat oldFormat = new SimpleDateFormat(dateFormatOld);
            date = oldFormat.parse(oriValue);

         }

         if (date != null) {
            String newValue = "";
            if ("stamp".equals(dateFormatNew)) {
               long ts = date.getTime();
               newValue = String.valueOf(ts);
            } else {
               SimpleDateFormat newFormat = new SimpleDateFormat(dateFormatNew);
               newValue = newFormat.format(date);
            }

            record.setColumn(columnIndex, new StringColumn(newValue));
         }

      } catch (ParseException e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
      }
      return record;
   }

   public static void main(String[] args) throws ParseException {
//      SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//      Date date = simpleDateFormat.parse("2020-09-15 12:11:11");
//      long ts = date.getTime();
//      System.out.println(ts);


      SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      long lt = new Long("1601374220000");
      Date date = new Date(lt);
      String format = simpleDateFormat.format(date);
      StringColumn stringColumn = new StringColumn(format);
      Date date1 = stringColumn.asDate();
      System.out.println(date1);
   }
}
