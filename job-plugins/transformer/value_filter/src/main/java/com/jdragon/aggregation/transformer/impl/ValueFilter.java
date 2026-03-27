package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.DateColumn;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.common.StringDateFormatEnum;
import org.apache.commons.lang3.StringUtils;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * <p>字符串日期处理
 * </p>
 *
 * @author jdragon
 * @since 2026/3/27
 */
public class ValueFilter extends Transformer {

    int columnIndex;
    String stringFormat = "";
    String toDate = "";
    String[] possiblePatterns =
            {
                    "yyyy-MM-dd",
                    "yyyy-MM-dd HH:mm:ss",
                    "yyyyMMdd",
                    "yyyyMMdd HH:mm:ss",
                    "yyyy/MM/dd",
                    "yyyy/MM/dd HH:mm:ss",
                    "yyyy年MM月dd日",
                    "yyyy年MM月dd日 HH:mm:ss",
                    "yyyy MM dd",
                    "yyyy MM dd HH:mm:ss"
            };

    public ValueFilter() {
        super.setTransformerName("value_filter");
    }

    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            if (paras.length != 3) {
                throw new RuntimeException("value_filter paras must be 3");
            }
            columnIndex = (Integer) paras[0];
            stringFormat = (String) paras[1];
            toDate = (String) paras[2];
        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_ILLEGAL_PARAMETER, "paras:" + Arrays.asList(paras).toString() + " => " + e.getMessage());
        }
        Record newRecord = null;
        newRecord = dateFormat(record);
        return newRecord;
    }

    /**
     * 字符串类型日期统一使用日期/时间格式
     *
     * @param record
     * @return
     */
    public Record dateFormat(Record record) {
        try {
            Column column = record.getColumn(columnIndex);

            if (stringFormat.equalsIgnoreCase("timestamp")) {
                long time = column.asLong();
                if (time < 1000000000000L) {
                    time = time * 1000;
                }
                record.setColumn(columnIndex, new DateColumn(time));
                return record;
            }

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(stringFormat);
            String s = column.asString();
            if (StringUtils.isBlank(s)) {
                return record;
            }
            Date date = simpleDateFormat.parse(s);
            if (date != null) {
                if (toDate.equalsIgnoreCase(StringDateFormatEnum.DATE.name())) {
                    java.sql.Date date1 = new java.sql.Date(date.getTime());
                    record.setColumn(columnIndex, new DateColumn(date1));
                } else if (toDate.equalsIgnoreCase(StringDateFormatEnum.DATETIME.name())) {
                    Timestamp timestamp = new Timestamp(date.getTime());
                    record.setColumn(columnIndex, new DateColumn(timestamp));
                } else if (toDate.equalsIgnoreCase(StringDateFormatEnum.TIME.name())) {
                    Time time = new Time(date.getTime());
                    record.setColumn(columnIndex, new DateColumn(time));
                }

            }
        } catch (ParseException e) {
            record.setColumn(columnIndex, new DateColumn());
        }
        return record;

//        Column column = record.getColumn(columnIndex);
//        Date date = null;
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat();
//        for (String possiblePattern : possiblePatterns) {
//            simpleDateFormat.applyPattern(possiblePattern);
//            simpleDateFormat.setLenient(false);//设置解析日期格式是否严格解析日期
//            Date date1 = simpleDateFormat.parse(column.asString(), new ParsePosition(0));
//           if (date1!=null){
//               date=date1;
//           }
//        }
//        if (date!=null){
//            long time = date.getTime();
//            SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//            String format = sf.format(new Date(time));
//            record.setColumn(columnIndex,new StringColumn(format));
//        }
//        return record;
    }

    public static void main(String[] args) throws ParseException {
        String str = "1669365871000";
        Timestamp timestamp = new Timestamp(Long.parseLong(str));
        DateColumn dateColumn = new DateColumn(timestamp);
        System.out.println(dateColumn.asString());
    }
}
