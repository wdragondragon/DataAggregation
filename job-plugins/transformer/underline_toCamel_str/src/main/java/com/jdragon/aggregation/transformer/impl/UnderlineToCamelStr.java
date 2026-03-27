package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.transformer.TransformerErrorCode;
import com.jdragon.aggregation.core.plugin.Transformer;
import org.apache.commons.lang3.StringUtils;

/**
 * <p>
 * <p>下划线转驼峰
 * </p>
 *
 * @author jdragon
 * @since 2026/3/27
 */
public class UnderlineToCamelStr extends Transformer {
    public UnderlineToCamelStr() {
        super.setTransformerName("underline_toCamel_str");
    }
    /**
     * 下划线字符
     */
    public static final char UNDERLINE = '_';
    int columnIndex;
    @Override
    public Record evaluate(Record record, Object... paras) {
        try {
            columnIndex = (Integer)paras[0];
            Column column = record.getColumn(columnIndex);
            String oriValue = column.asString();
            if (StringUtils.isBlank(oriValue)){
                return record;
            }
            String newValue = underlineToCamel(oriValue);
            record.setColumn(columnIndex,new StringColumn(newValue));
        } catch (Exception e) {
            throw AggregationException.asException(TransformerErrorCode.TRANSFORMER_RUN_EXCEPTION, e.getMessage(),e);

        }
        return record;
    }

    /**
     * 字符串下划线转驼峰格式
     *
     * @param param 需要转换的字符串
     * @return 转换好的字符串
     */
    public static String underlineToCamel(String param) {
        //String temp = param.toLowerCase();
        int len = param.length();
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char c = param.charAt(i);
            if (c == UNDERLINE) {
                if (++i < len) {
                    sb.append(Character.toUpperCase(param.charAt(i)));
                }
            } else {
                sb.append(c);
            }
        }

        return sb.toString();
    }
}
