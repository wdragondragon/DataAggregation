package com.jdragon.aggregation.core.transport.record;

import com.alibaba.fastjson.JSON;
import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.core.utils.ClassSize;
import com.jdragon.aggregation.core.utils.FrameworkErrorCode;
import lombok.Getter;
import lombok.Setter;

import java.util.*;


public class DefaultRecord implements Record, Cloneable {

    private static final int RECORD_AVERGAE_COLUMN_NUMBER = 16;

    @Getter
    @Setter
    private List<Column> columns;

    private int byteSize;

    // 首先是Record本身需要的内存
    private int memorySize = ClassSize.DefaultRecordHead;


    @Override
    public void setExtraColumn(String key, Column column) {
        if (extraColumn.containsKey(key)) {
            decrByteSize(extraColumn.get(key));
        }
        incrByteSize(column);
        extraColumn.put(key, column);
    }

    public DefaultRecord() {
        this.columns = new ArrayList<Column>(RECORD_AVERGAE_COLUMN_NUMBER);
    }

    @Override
    public void addColumn(Column column) {
        columns.add(column);
        incrByteSize(column);
    }

    @Override
    public Column getColumn(int i) {
        if (i < 0 || i >= columns.size()) {
            return null;
        }
        return columns.get(i);
    }

    @Override
    public void setColumn(int i, final Column column) {
        if (i < 0) {
            throw AggregationException.asException(FrameworkErrorCode.ARGUMENT_ERROR,
                    "不能给index小于0的column设置值");
        }

        if (i >= columns.size()) {
            expandCapacity(i + 1);
        }

        decrByteSize(getColumn(i));
        this.columns.set(i, column);
        incrByteSize(getColumn(i));
    }

    @Override
    public String toString() {
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("size", this.getColumnNumber());
        json.put("data", this.columns);
        return JSON.toJSONString(json);
    }

    @Override
    public int getColumnNumber() {
        return this.columns.size();
    }

    @Override
    public int getByteSize() {
        return byteSize;
    }

    @Override
    public int getMemorySize() {
        return memorySize;
    }

    @Override
    public void removeColumn(Integer... i) {
        if (i == null || i.length == 0) {
            return;
        }
        // 逆序删除，防止index错乱
        Arrays.sort(i, Collections.reverseOrder());
        for (int index : i) {
            removeColumn(index);
        }
    }

    @Override
    public void removeColumn(int i) {
        if (i < 0 || i >= columns.size()) {
            return;
        }
        decrByteSize(getColumn(i));
        this.columns.remove(i);
    }

    private void decrByteSize(final Column column) {
        if (null == column) {
            return;
        }

        byteSize -= column.getByteSize();

        //内存的占用是column对象的头 再加实际大小
        memorySize = memorySize - ClassSize.ColumnHead - column.getByteSize();
    }

    private void incrByteSize(final Column column) {
        if (null == column) {
            return;
        }

        byteSize += column.getByteSize();

        //内存的占用是column对象的头 再加实际大小
        memorySize = memorySize + ClassSize.ColumnHead + column.getByteSize();
    }

    private void expandCapacity(int totalSize) {
        if (totalSize <= 0) {
            return;
        }

        int needToExpand = totalSize - columns.size();
        while (needToExpand-- > 0) {
            this.columns.add(null);
        }
    }

    @Override
    public DefaultRecord clone() throws CloneNotSupportedException {
        DefaultRecord clone = (DefaultRecord) super.clone();
        List<Column> list = new ArrayList<Column>();
        for (Column column : columns) {
            list.add(column.clone());
        }
        clone.columns = list;
        return (DefaultRecord) super.clone();
    }

    public static int getRecordAvergaeColumnNumber() {
        return RECORD_AVERGAE_COLUMN_NUMBER;
    }

    @Override
    public boolean equals(Object o) {
        DefaultRecord record = (DefaultRecord) o;
        List<Column> columns = record.getColumns();
        List<Column> columnList = this.getColumns();
        int falseNum = 0;
        for (int i = 0; i < columns.size(); i++) {
            if (!columns.get(i).equals(columnList.get(i))) {
                falseNum++;
            }
        }
        return falseNum == 0;
    }

}
