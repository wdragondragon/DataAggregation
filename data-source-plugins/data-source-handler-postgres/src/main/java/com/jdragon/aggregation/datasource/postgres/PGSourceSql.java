package com.jdragon.aggregation.datasource.postgres;

import com.jdragon.aggregation.datasource.rdbms.DefaultSourceSql;
import lombok.Getter;

@Getter
public class PGSourceSql extends DefaultSourceSql {

    private final String dataPreview = "select * from {0} limit {1}";

    private final String tableSize = "SELECT ROUND(pg_total_relation_size(c.oid) / 1024 / 1024.0, 2) AS \"data\"\n" +
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace\n" +
            "WHERE n.nspname = ''{0}'' AND c.relname = ''{1}''";

}
