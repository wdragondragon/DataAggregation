package com.jdragon.aggregation.datasource;

public interface IDataSourceSql {

    String DATA_PREVIEW = "select * from {0} limit {1}";

    String TRUNCATE_TABLE = "truncate table {0}";

    String INSERT_DATA = "insert into {0}({1}) values {2}";

    String MYSQL_QUERY_TABLE_SIZE = "SELECT ROUND((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024, 2) AS data FROM information_schema.TABLES WHERE table_schema =''{0}'' and table_name = ''{1}''";

    String TABLE_COUNT = "SELECT COUNT(*) AS table_count FROM {0}";

    default String getDataPreview() {
        return DATA_PREVIEW;
    }

    default String getTruncateTable() {
        return TRUNCATE_TABLE;
    }

    default String getInsertData() {
        return INSERT_DATA;
    }

    default String getTableSize() {
        return MYSQL_QUERY_TABLE_SIZE;
    }

    default String getTableCount() {
        return TABLE_COUNT;
    }
}
