package com.jdragon.aggregation.dm;

import com.jdragon.aggregation.datasource.dm.DMSourcePlugin;
import com.jdragon.aggregation.rdbms.reader.CommonRdbmsReader;

public class DmReader extends CommonRdbmsReader {

    public DmReader() {
        super(new DMSourcePlugin());
    }

}
