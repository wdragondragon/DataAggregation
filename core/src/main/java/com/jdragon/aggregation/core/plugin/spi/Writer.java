package com.jdragon.aggregation.core.plugin.spi;


import com.jdragon.aggregation.core.plugin.AbstractJobPlugin;
import com.jdragon.aggregation.core.plugin.RecordReceiver;

/**
 * 每个Writer插件需要实现Writer类，并在其内部实现Job、Task两个内部类。
 */
public abstract class Writer {
    /**
     * 每个Writer插件必须实现Job内部类
     */
    public abstract static class Job extends AbstractJobPlugin {
        public abstract void startWrite(RecordReceiver lineReceiver);

    }
}
