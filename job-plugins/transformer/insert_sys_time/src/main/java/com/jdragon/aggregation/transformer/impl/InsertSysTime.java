package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.DateColumn;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.groovy.runtime.powerassert.SourceText;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 * @author jdragon
 * @date 2026/3/27
 * 添加系统时间
 */
@Slf4j
public class InsertSysTime extends Transformer {
   public InsertSysTime() {
      setTransformerName("insert_sys_time");
   }

   @Override
   public Record evaluate(Record record, Object... paras) {
      int columnIndex;
      String format;
      try {
         if (paras.length != 2) {
            throw new RuntimeException("insert_sys_time paras must be 2");
         }

         columnIndex = (Integer) paras[0];
         format = (String) paras[1];
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
      }

      try {
         //如果字段为空，跳过replace处理
         SimpleDateFormat sdf = new SimpleDateFormat(format);
         String time = sdf.format(new Date());
         record.setColumn(columnIndex, new StringColumn(time));
      } catch (Exception e) {
         throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(), e);
      }
      return record;
   }

   public static void main(String[] args) throws ParseException {
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
      String time = simpleDateFormat.format(new Date());
      Date parse = simpleDateFormat.parse(time);
      System.out.println(time);
      System.out.println(parse);
   }
}
