package com.jdragon.aggregation.datasource;

public interface IDataSourceSql {

    String DATA_PREVIEW = "select * from {0} limit {1}";

    String TRUNCATE_TABLE = "truncate table {0}";

    String INSERT_DATA = "insert into {0}({1}) values {2}";

    default String getDataPreview() {
        return DATA_PREVIEW;
    }

    default String getTruncateTable() {
        return TRUNCATE_TABLE;
    }

    default String getInsertData() {
        return INSERT_DATA;
    }

}
