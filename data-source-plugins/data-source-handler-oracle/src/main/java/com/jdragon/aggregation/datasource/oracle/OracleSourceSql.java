package com.jdragon.aggregation.datasource.oracle;

import com.jdragon.aggregation.datasource.rdbms.DefaultSourceSql;
import lombok.Getter;

@Getter
public class OracleSourceSql extends DefaultSourceSql {

    private final String dataPreview = "select * from {0} where ROWNUM<={1}";

}
