package com.jdragon.aggregation.datasource.tbds.hive2;

import com.jdragon.aggregation.datasource.DataSourceType;
import com.jdragon.aggregation.datasource.rdbms.DefaultSourceSql;
import com.jdragon.aggregation.datasource.rdbms.RdbmsSourcePlugin;
import lombok.Getter;

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
        super(new DefaultSourceSql());
    }
}
