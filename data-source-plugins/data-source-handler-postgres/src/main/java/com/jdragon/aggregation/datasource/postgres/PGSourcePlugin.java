package com.jdragon.aggregation.datasource.postgres;

import com.jdragon.aggregation.datasource.DataSourceType;
import com.jdragon.aggregation.datasource.rdbms.DefaultSourceSql;
import com.jdragon.aggregation.datasource.rdbms.RdbmsSourcePlugin;
import lombok.Getter;

@Getter
public class PGSourcePlugin extends RdbmsSourcePlugin {

    private final DataSourceType type = DataSourceType.PostgreSQL;

    private final String driver = DataSourceType.PostgreSQL.getDriverClassName();

    private final String jdbc = "jdbc:postgresql://%s:%s/%s";

    private final String testQuery = "select 1";

    private final String quotationMarks = "\"";

    private final String separator = "&";

    private final String extraParameterStart = "?";

    public PGSourcePlugin() {
        super(new DefaultSourceSql());
    }
}
