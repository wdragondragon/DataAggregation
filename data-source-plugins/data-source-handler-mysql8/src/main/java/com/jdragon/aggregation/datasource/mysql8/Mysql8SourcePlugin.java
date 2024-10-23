package com.jdragon.aggregation.datasource.mysql8;

import com.jdragon.aggregation.datasource.DataSourceType;
import com.jdragon.aggregation.datasource.rdbms.RdbmsSourcePlugin;
import lombok.Getter;


@Getter
public class Mysql8SourcePlugin extends RdbmsSourcePlugin {

    private final DataSourceType type = DataSourceType.Mysql8;

    private final String driver = DataSourceType.Mysql8.getDriverClassName();

    private final String jdbc = "jdbc:mysql://%s:%s/%s";

    private final String testQuery = "select 1";

    private final String quotationMarks = "`";

    private final String separator = "&";

    private final String extraParameterStart = "?";

    public Mysql8SourcePlugin() {
        super(new MysqlSql());
    }
}
