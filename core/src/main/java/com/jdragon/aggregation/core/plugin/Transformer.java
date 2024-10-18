package com.jdragon.aggregation.core.plugin;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;

public abstract class Transformer extends AbstractPlugin {
    private String transformerName;

    public String getTransformerName() {
        return transformerName;
    }

    public void setTransformerName(String transformerName) {
        this.transformerName = transformerName;
    }

    /**
     * @param record 行记录，UDF进行record的处理后，更新相应的record
     * @param paras  transformer函数参数
     */
    abstract public Record evaluate(Record record, Object... paras);
}
