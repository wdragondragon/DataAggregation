package com.jdragon.aggregation.core.plugin;

import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

@Setter
@Getter
public abstract class Transformer extends AbstractPlugin {

    private String transformerName;

    /**
     * @param record 行记录，UDF进行record的处理后，更新相应的record
     * @param paras  transformer函数参数
     */
    abstract public Record evaluate(Record record, Object... paras);

    public Record evaluate(Record record, Map<String, Object> tContext, Object... paras) {
        return evaluate(record, paras);
    }
}
