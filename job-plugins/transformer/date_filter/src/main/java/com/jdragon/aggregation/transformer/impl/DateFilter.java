package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.DateColumn;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.core.plugin.Transformer;
import com.jdragon.aggregation.transformer.util.ValidateUtil;
import org.apache.commons.lang3.StringUtils;

import java.sql.Time;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * <p>
 * <p>日期转换成时间戳,非法日期置空
 * </p>
 *
 * @author jdragon
 * @since 2026/3/27
 */
public class DateFilter extends Transformer {

    int columnIndex;
    String dateFormat;
    public DateFilter(){
        super.setTransformerName("date_filter");
    }
    @Override
    public Record evaluate(Record record, Object... paras) {
        columnIndex=(Integer) paras[0];
        dateFormat = (String) paras[1];
        return dateFormatTimestamp(record);
    }

    /**
     * 日期格式转换成时间戳，非法日期置空；不是以上格式string为非法日期
     * @param record
     * @return
     */
    public Record dateFormatTimestamp(Record record) {
            Column column = record.getColumn(columnIndex);
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        Date date = null;
        try {
            String s = column.asString();
            if (StringUtils.isBlank(s)){
                return record;
            }
            date = simpleDateFormat.parse(s);
            if (date!=null){
                long time = date.getTime();
                record.setColumn(columnIndex,new DateColumn(time));
            }

        } catch (ParseException e) {
            record.setColumn(columnIndex,new DateColumn());
        }
        return record;
    }
}
