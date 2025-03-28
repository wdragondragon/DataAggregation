package com.jdragon.aggregation.plugin.httpdyn.impl;


import com.jdragon.aggregation.plugin.httpdyn.BaseHttpDynExecutor;

/**
 * 获取时间戳
 * @author hjs
 * @version 1.0
 * @date 2020/5/5 19:20
 */
public class GetNowTimeStamp extends BaseHttpDynExecutor {

    public GetNowTimeStamp() {
        super.setHttpDynName("dyn_timestamp");
    }

    @Override
    public String execute(Object... paras) {
      return String.valueOf(System.currentTimeMillis());
    }
}
