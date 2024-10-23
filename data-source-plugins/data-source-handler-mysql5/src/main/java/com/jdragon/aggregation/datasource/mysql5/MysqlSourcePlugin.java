package com.jdragon.aggregation.datasource.mysql5;

import com.jdragon.aggregation.datasource.DataSourceType;
import com.jdragon.aggregation.datasource.IDataSourceSql;
import com.jdragon.aggregation.datasource.rdbms.RdbmsSourcePlugin;
import lombok.Getter;


@Getter
public class MysqlSourcePlugin extends RdbmsSourcePlugin {

    private final DataSourceType type = DataSourceType.MySql;

    private final String driver = DataSourceType.MySql.getDriverClassName();

    private final String jdbc = "jdbc:mysql://%s:%s/%s";

    private final String testQuery = "select 1";

    private final String quotationMarks = "`";

    private final String separator = "&";

    private final String extraParameterStart = "?";

    public MysqlSourcePlugin() {
        super(new MysqlSql());
    }
}
