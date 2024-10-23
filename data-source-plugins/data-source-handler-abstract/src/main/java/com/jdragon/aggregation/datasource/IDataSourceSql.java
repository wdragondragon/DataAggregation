package com.jdragon.aggregation.datasource;

public interface IDataSourceSql {

    String DATA_PREVIEW = "select * from {0} limit {1}";

    default String getDataPreview() {
        return DATA_PREVIEW;
    }
}
