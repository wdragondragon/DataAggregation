package com.jdragon.aggregation.writer.dm;

import com.jdragon.aggregation.datasource.dm.DMSourcePlugin;
import com.jdragon.aggregation.rdbms.writer.CommonRdbmsWriter;

public class DmWriter extends CommonRdbmsWriter {
    public DmWriter() {
        super(new DMSourcePlugin());
    }
}
