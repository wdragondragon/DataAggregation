package com.jdragon.aggregation.tbds.hive2;

import com.jdragon.aggregation.datasource.tbds.hive2.Hive2SourcePlugin;
import com.jdragon.aggregation.rdbms.reader.CommonRdbmsReader;

public class Hive2Reader extends CommonRdbmsReader {
    public Hive2Reader() {
        super(new Hive2SourcePlugin());
    }
}
