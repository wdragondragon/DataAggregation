package com.jdragon.aggregation.plugin.httpdyn.impl;


import com.jdragon.aggregation.plugin.httpdyn.BaseHttpDynExecutor;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * @author tfq
 * @date 2020/5/19 10:11
 */
public class Sha1 extends BaseHttpDynExecutor {
   public Sha1() {
      super.setHttpDynName("dyn_Sha1");
   }
   @Override
   public String execute(Object... paras) {
      String oriValue = "";
      if(isParamIsNotBank(paras)) {
         if (paras[0] != null) {
            String param0 = paras[0].toString();
            if (StringUtils.isNotBlank(param0)) {
               oriValue = DigestUtils.sha1Hex(param0);
            }
         }
      }
      return oriValue;
   }
}
