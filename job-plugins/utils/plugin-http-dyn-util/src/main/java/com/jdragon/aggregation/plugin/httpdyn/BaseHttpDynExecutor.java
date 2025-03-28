package com.jdragon.aggregation.plugin.httpdyn;

import lombok.Getter;
import lombok.Setter;

/**
 * @author hjs
 * @version 1.0
 */
@Setter
@Getter
public abstract class BaseHttpDynExecutor {

    /**
     * 动态参数名称
     */
    private String httpDynName;


    /**
     * 判断传入的参数是否为空
     *
     * @param paras 传入参数
     * @return 返回是否为空
     */
    protected final Boolean isParamIsNotBank(Object... paras) {
        if (null != paras && paras.length > 0) {
            return true;
        }
        return false;
    }

    /**
     * 执行方法
     *
     * @return 用于替换的字符串
     */
    abstract public String execute(Object... paras);
}
