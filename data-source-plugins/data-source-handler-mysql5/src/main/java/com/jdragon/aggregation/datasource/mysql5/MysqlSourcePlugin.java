package com.jdragon.aggregation.datasource.mysql5;

import com.jdragon.aggregation.datasource.DataSourceType;
import com.jdragon.aggregation.datasource.IDataSourceSql;
import com.jdragon.aggregation.datasource.rdbms.DefaultSourceSql;
import com.jdragon.aggregation.datasource.rdbms.RdbmsSourcePlugin;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.Map;


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
        super(new DefaultSourceSql());
    }

    @Override
    protected Map<String, String> defaultJdbcParameters() {
        Map<String, String> parameters = new LinkedHashMap<>();
        parameters.put("useUnicode", "true");
        parameters.put("characterEncoding", "UTF-8");
        return parameters;
    }
}
