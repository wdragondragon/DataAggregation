package com.jdragon.aggregation.core.plugin.spi;


import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import com.jdragon.aggregation.core.plugin.RecordSender;

public abstract class Reader {

    public static abstract class Job extends AbstractJobPlugin {
        public abstract void startRead(RecordSender recordSender);

    }
}
