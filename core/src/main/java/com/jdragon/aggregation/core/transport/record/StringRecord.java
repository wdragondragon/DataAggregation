package com.jdragon.aggregation.core.transport.record;

import com.jdragon.aggregation.commons.element.Column;
import com.jdragon.aggregation.commons.element.StringColumn;

public class StringRecord extends DefaultRecord {

    public StringRecord(String record) {
        super();
        super.addColumn(new StringColumn(record));
    }

    @Override
    public void addColumn(Column column) {
        super.removeColumn(0);
        super.addColumn(column);
    }

    @Override
    public void setColumn(int i, Column column) {
        super.addColumn(column);
    }

    @Override
    public Column getColumn(int i) {
        return super.getColumn(0);
    }

    @Override
    public String toString() {
        return super.getColumn(0).toString();
    }
}
