package com.jdragon.aggregation.plugin.httpdyn.impl;


import com.jdragon.aggregation.plugin.httpdyn.BaseHttpDynExecutor;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author tfq
 * @date 2020/5/19 10:10
 */
public class Md5 extends BaseHttpDynExecutor {
    public Md5() {
        super.setHttpDynName("dyn_MD5");
    }

    @Override
    public String execute(Object... paras) {
        String oriValue = "";
        if (isParamIsNotBank(paras)) {
            if (paras[0] != null) {
                String param0 = paras[0].toString();
                if (StringUtils.isNotBlank(param0)) {
                    oriValue = DigestUtils.md5Hex(param0);
                }
            }
        }
        return oriValue;
    }
}
