package com.jdragon.aggregation.datasource.tbds.hive2;

public interface SQLConstants {
    String HIVE_TABLE_SIZE = "show tblproperties `{0}`";
    String HIVE_ANALYEZE_TABLE = "analyze table {0}  COMPUTE STATISTICS noscan";
    String HIVE_PRPT_NAME = "prpt_name";
    String HIVE_TOTAL_SIZE = "totalSize";
    String HIVE_PRPT_VALUE = "prpt_value";
}
