package com.jdragon.aggregation.plugin.httpdyn.enums;

/**
 * @author hjs
 * @version 1.0
 * @date 2020/5/5 23:37
 */
public enum HttpDynConfigEnum {
    /**
     * 所有枚举类，后面会改成使用插件的方式，现在先写死在枚举类
     */
    MD5("<dyn_MD5>", "com.jdragon.aggregation.plugin.httpdyn.impl.Md5", ""),
    SHA1("<dyn_Sha1>", "com.jdragon.aggregation.plugin.httpdyn.impl.Sha1", ""),
    SHA256("<dyn_Sha256>", "com.jdragon.aggregation.plugin.httpdyn.impl.Sha256", ""),
    SHA512("<dyn_Sha512>", "com.jdragon.aggregation.plugin.httpdyn.impl.Sha512", ""),
    BM_GET_NOW_TIMESTAMP("<dyn_timestamp>", "com.bmsoft.dc.plugin.httpdyncolumn.impl.BmGetNowTimeStamp", "获取当前时间时间戳"),
    BM_DYN_FROM_HTTP_TOKEN("<dyn_from_http_token>","com.jdragon.aggregation.plugin.httpdyn.impl.FromHttpToken","动态获取token"),
    BM_DYN_TIMESTAMP("<dyn_ten_timestamp>","com.jdragon.aggregation.plugin.httpdyn.impl.GetNowTenTimeStamp",""),
    ;

    /**
     * 对应key
     */
    private String bmCode;

    /**
     * 需要动态加载的类名
     */
    private String className;

    /**
     * 描述
     */
    private String desc;

    HttpDynConfigEnum(String bmCode, String className, String desc) {
        this.bmCode = bmCode;
        this.className = className;
        this.desc = desc;
    }

    public String getBmCode() {
        return bmCode;
    }

    public void setBmCode(String bmCode) {
        this.bmCode = bmCode;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getDesc() {
        return desc;
    }

    public void setDesc(String desc) {
        this.desc = desc;
    }

    /**
     * 根据code获取类名
     * @param bmCode
     * @return
     */
    public static String getClassNameByCode(String bmCode) {
        for (HttpDynConfigEnum param : HttpDynConfigEnum.values()) {
            if (bmCode.equals(param.getBmCode())) {
                return param.getClassName();
            }
        }
        return "";
    }
}
