package com.jdragon.aggregation.datasource.scan;

import com.jdragon.aggregation.datasource.BaseDataSourceDTO;

import java.util.Map;
import java.util.function.Consumer;

public interface TableScanReadable {
    void scanRows(BaseDataSourceDTO dataSource, String querySql, boolean columnLabel, Consumer<Map<String, Object>> rowConsumer);
}
