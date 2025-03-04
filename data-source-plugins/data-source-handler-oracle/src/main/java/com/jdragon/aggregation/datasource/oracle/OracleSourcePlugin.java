package com.jdragon.aggregation.datasource.oracle;

import com.jdragon.aggregation.datasource.DataSourceType;
import com.jdragon.aggregation.datasource.rdbms.RdbmsSourcePlugin;
import lombok.Getter;

@Getter
public class OracleSourcePlugin extends RdbmsSourcePlugin {

    private final DataSourceType type = DataSourceType.Oracle;

    private final String driver = DataSourceType.Oracle.getDriverClassName();

    private final String jdbc = "jdbc:oracle:thin:@//%s:%s/%s";

    private final String testQuery = "select 1";

    private final String quotationMarks = "\"";

    private final String separator = "&";

    private final String extraParameterStart = "?";

    public OracleSourcePlugin() {
        super(new OracleSourceSql());
    }
}
