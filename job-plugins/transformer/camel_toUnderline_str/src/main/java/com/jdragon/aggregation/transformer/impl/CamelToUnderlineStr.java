package com.jdragon.aggregation.transformer.impl;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.core.plugin.Transformer;
import org.apache.commons.lang3.StringUtils;

/**
 * <p>
 * <p>
 * </p>驼峰转下划线
 *
 * @author jdragon
 * @since 2026/3/27
 */
public class CamelToUnderlineStr extends Transformer {
    public CamelToUnderlineStr() {
        super.setTransformerName("camel_toUnderline_str");
    }

    int columnIndex;
    public static final char UNDERLINE = '_';
    @Override
    public Record evaluate(Record record, Object... paras) {
        columnIndex = (Integer)paras[0];
        Column column = record.getColumn(columnIndex);
        String oriValue = column.asString();
        if (StringUtils.isBlank(oriValue)){
            return record;
        }
        String newValue = camelToUnderline(oriValue);
        record.setColumn(columnIndex,new StringColumn(newValue));
        return record;
    }

    /**
     * 字符串驼峰转下划线格式
     *
     * @param oriValue 需要转换的字符串
     * @return 转换好的字符串
     */
    public static String camelToUnderline(String oriValue) {
        int len = oriValue.length();
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char c = oriValue.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
                sb.append(UNDERLINE);
            }
            sb.append(Character.toLowerCase(c));
        }
        return sb.toString();
    }

}
