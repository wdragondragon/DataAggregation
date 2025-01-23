package com.jdragon.aggregation.core.transformer;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * no comments.
 * Created by liqiang on 16/3/16.
 */
@Setter
@Getter
public class TransformerExecutionParas {

    /**
     * 以下是function参数
     */

    private Integer columnIndex;

    private String[] paras;

    private Map<String, Object> tContext;

    private String code;

    private List<String> extraPackage;

}
