package com.jdragon.aggregation.writer.mysql5;

import com.jdragon.aggregation.datasource.mysql5.MysqlSourcePlugin;
import com.jdragon.aggregation.rdbms.writer.CommonRdbmsWriter;

public class Mysql5Writer extends CommonRdbmsWriter {
    public Mysql5Writer() {
        super(new MysqlSourcePlugin());
    }
}
