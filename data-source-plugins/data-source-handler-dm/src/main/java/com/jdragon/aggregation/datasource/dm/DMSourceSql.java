package com.jdragon.aggregation.datasource.dm;

import com.jdragon.aggregation.datasource.rdbms.DefaultSourceSql;
import lombok.Getter;

@Getter
public class DMSourceSql extends DefaultSourceSql {

    private final String dataPreview = "select * from {0} where ROWNUM<={1}";

    private final String tableSize = "select ROUND(BYTES*1.0/1024/1024,2) as \"data\" from dba_segments where segment_type = ''TABLE'' and OWNER=''{0}'' and segment_name=''{1}''";

    private final String tableCount = "SELECT COUNT(*) AS \"table_count\" FROM {0}";

}
