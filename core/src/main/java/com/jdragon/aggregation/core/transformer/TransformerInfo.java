package com.jdragon.aggregation.core.transformer;

import com.jdragon.aggregation.core.plugin.Transformer;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class TransformerInfo {

    /**
     * function基本信息
     */
    private Transformer transformer;

    private ClassLoader classLoader;

    private boolean isNative;
}
