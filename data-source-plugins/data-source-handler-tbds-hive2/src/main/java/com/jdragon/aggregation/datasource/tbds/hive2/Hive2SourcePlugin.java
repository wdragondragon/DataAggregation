package com.jdragon.aggregation.datasource.tbds.hive2;

import com.google.common.collect.Lists;
import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.DataSourceType;
import com.jdragon.aggregation.datasource.rdbms.RdbmsSourcePlugin;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.text.MessageFormat;
import java.util.*;

@Slf4j
@Getter
public class Hive2SourcePlugin extends RdbmsSourcePlugin {
    private final DataSourceType type = DataSourceType.HIVE;

    private final String driver = "org.apache.hive.jdbc.HiveDriver";

    private final String jdbc = "jdbc:hive2://%s:%s/%s";

    private final String testQuery = "select 1";

    private final String quotationMarks = "`";

    private final String schema = "";

    private final String tableSpace = "";

    private final String separator = ";";

    public Hive2SourcePlugin() {
        super(new HiveSourceSql());
    }

    @Override
    public String getTableSize(BaseDataSourceDTO dataSource, String table) {
        String analyzeSql = MessageFormat.format(SQLConstants.HIVE_ANALYEZE_TABLE, table);
        try {
            executeUpdate(dataSource, analyzeSql);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        String sql = MessageFormat.format(SQLConstants.HIVE_TABLE_SIZE, table);
        float dataLength = 0.0f;
        Table<Map<String, Object>> executed = executeQuerySql(dataSource, sql, true);
        for (Map<String, Object> next : executed.getBodies()) {
            if (next.get(SQLConstants.HIVE_PRPT_NAME).equals(SQLConstants.HIVE_TOTAL_SIZE)) {
                dataLength = dataLength + Float.parseFloat(next.get(SQLConstants.HIVE_PRPT_VALUE).toString());
            }
        }
        return String.valueOf(dataLength);
    }
}
