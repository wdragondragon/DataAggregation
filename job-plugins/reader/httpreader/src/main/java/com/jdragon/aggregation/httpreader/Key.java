package com.jdragon.aggregation.httpreader;

/**
 * <p>
 * <p>
 * </p>
 *
 * @author lxs
 * @since 2020/2/28
 */
public final class Key {

    public final static String COLUMN = "columns";
    public final static String TYPE = "type";
    public final static String NAME = "name";

    //请求类型:GET、POST
    public final static String MODE = "mode";
    //请求url
    public final static String URL = "url";
    public final static String CONTENT_TYPE = "contentType";
    public final static String HEADER = "header";
    //返回类型
    public final static String REQUEST_TYPE = "requestType";
    //请求参数
    public final static String PARAM_STR = "params";
    //请求体
    public final static String REQUEST_BODY = "requestBody";
    //分页相关
    public final static String PAGE_READ = "pageRead";
    public final static String PAGE_SIZE = "pageSize";
    //总量
    public final static String TOTAL_CODE_PATH = "totalCodePath";
    //返回状态map,里面包含code,path
    public final static String RESPONSE_STATUS = "responseStatus";
    // public final static String WHERE = "where";
    //增量参数开始值
    public final static String INCREMENT_START = "incrementStart";
    //增量参数结束值
    public final static String INCREMENT_END = "incrementEnd";
    //增量值
    public final static String INCREMENT_VALUE = "incrementValue";
    //返回结果类型：xml,soap,json 默认json
    public final static String RESULT_DATA_TYPE = "resultType";


}

