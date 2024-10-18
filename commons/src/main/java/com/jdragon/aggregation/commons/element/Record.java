package com.jdragon.aggregation.commons.element;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jingxing on 14-8-24.
 */

public interface Record {

	default void removeColumn(Integer... i){
		throw new UnsupportedOperationException();
	}

	default void removeColumn(int i){
		throw new UnsupportedOperationException();
	}

	public void addColumn(Column column);

	public void setColumn(int i, final Column column);

	public Column getColumn(int i);

	public String toString();

	public int getColumnNumber();

	public int getByteSize();

	public int getMemorySize();

	Map<String, Column> extraColumn = new HashMap<>();

	default Column getExtraColumn(String key) {
		return extraColumn.get(key);
	}

	default void setExtraColumn(String key, Column column) {
		extraColumn.put(key, column);
	}

	default void setExtraColumn(Map<String, String> extraColumn) {
		if (extraColumn == null) {
			return;
		}
		for (Map.Entry<String, String> entry : extraColumn.entrySet()) {
			setExtraColumn(entry.getKey(), new StringColumn(entry.getValue()));
		}
	}
}
