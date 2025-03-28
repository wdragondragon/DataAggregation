package com.jdragon.aggregation.plugin.httpdyn.impl;


import com.jdragon.aggregation.plugin.httpdyn.BaseHttpDynExecutor;

import java.util.Date;

/**
 * @Author JDragon
 * @Date 2021.05.11 下午 4:36
 * @Email 1061917196@qq.com
 * @Des:
 */
public class GetNowTenTimeStamp extends BaseHttpDynExecutor {

    public GetNowTenTimeStamp() {
        super.setHttpDynName("dyn_ten_timestamp");
    }

    @Override
    public String execute(Object... paras) {
        long time = new Date().getTime();
        return String.valueOf(time).substring(0, 10);
    }
}
