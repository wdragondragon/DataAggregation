package com.jdragon.aggregation.writer.mysql8;

import com.jdragon.aggregation.datasource.mysql8.Mysql8SourcePlugin;
import com.jdragon.aggregation.rdbms.writer.CommonRdbmsWriter;

public class Mysql8Writer extends CommonRdbmsWriter {
    public Mysql8Writer() {
        super(new Mysql8SourcePlugin());
    }


}
