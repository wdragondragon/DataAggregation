package com.jdragon.aggregation.httpreader;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.util.FastJsonMemory;
import com.jdragon.aggregation.commons.util.RetryUtil;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.httpreader.utils.HttpUtils;
import com.jdragon.aggregation.plugin.httpdyn.HttpDynColumnExecuteHandler;
import com.jdragon.aggregation.unstructuredstorage.ColumnEntry;
import com.jdragon.aggregation.unstructuredstorage.SoapToJSON;
import com.jdragon.aggregation.unstructuredstorage.UnstructuredStorageReaderUtil;
import com.jdragon.aggregation.unstructuredstorage.XmlToJSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class HttpReader extends Reader.Job {
    private Configuration readerSliceConfig = null;

    private List<ColumnEntry> columnEntries = null;

    //请求路径
    private String url = "";
    //请求类型：post,get
    private String requestType = "";
    //响应参数类型
    private String resultDataType = "";
    //响应头信息
    private String contentType = "";

    private String headerStr = null;
    private String bodyStr = null;
    private String paramStr = null;
    private Map<String, String> httpContent = new HashMap<>();

    // 响应参数 包含响应状态码路径 和 正常状态码
    private Map<String, Object> responseStatus = null;

    // 页总数路径
    private String totalCodePath = null;

    private Long offset = 0L;

    //每页数据量
    private Integer pageSize;

    //是否分页读
    private Boolean pageRead = false;

    @Override
    public void init() {
        this.readerSliceConfig = super.getPluginJobConf();

        this.url = readerSliceConfig.getNecessaryValue(Key.URL, HttpReaderErrorCode.REQUIRED_VALUE);
        this.requestType = readerSliceConfig.getNecessaryValue(Key.MODE, HttpReaderErrorCode.REQUIRED_VALUE);
        this.contentType = readerSliceConfig.getUnnecessaryValue(Key.CONTENT_TYPE, HttpConstant.getContentType(), null);

        this.headerStr = readerSliceConfig.getString(Key.HEADER, "{}");
        this.paramStr = readerSliceConfig.getUnnecessaryValue(Key.PARAM_STR, "{}", null);
        this.bodyStr = readerSliceConfig.getString(Key.REQUEST_BODY, "{}");
        httpContent.put(Key.HEADER, headerStr);
        httpContent.put(Key.PARAM_STR, paramStr);
        httpContent.put(Key.REQUEST_BODY, bodyStr);

        this.resultDataType = readerSliceConfig.getUnnecessaryValue(Key.RESULT_DATA_TYPE, "json", null);


        this.totalCodePath = readerSliceConfig.getUnnecessaryValue(Key.TOTAL_CODE_PATH, null, null);
        this.responseStatus = readerSliceConfig.getMap(Key.RESPONSE_STATUS);

        this.columnEntries = UnstructuredStorageReaderUtil.getListColumnEntry(readerSliceConfig, Key.COLUMN);


        //是否分页
        this.pageSize = readerSliceConfig.getInt(Key.PAGE_SIZE, 500);
        this.pageRead = readerSliceConfig.getBool(Key.PAGE_READ, false);
    }

    @Override
    public void startRead(RecordSender recordSender) {
        Integer pageNum = 1;
        String resultStr = connectToGetData(new HashMap<>(httpContent), pageNum);

        FastJsonMemory resultJson = new FastJsonMemory(resultStr);
        Integer recordNum = sendAnalysisDataToWriter(resultJson, recordSender);
        if (this.pageRead) {
            log.info("进入pageRead读取模式");
            while (checkPage(pageNum, recordNum, resultJson)) {
                pageNum++;
                offset += pageSize;
                try {
                    resultStr = connectToGetData(new HashMap<>(httpContent), pageNum);
                    resultJson = new FastJsonMemory(resultStr);
                    recordNum = sendAnalysisDataToWriter(resultJson, recordSender);
                } catch (Exception e) {
                    log.info("转换异常，数据为：{}", resultStr);
                    throw new AggregationException(HttpReaderErrorCode.RESULT_RO_JSON_ERROR, "响应转换json异常");
                }
            }
        }

    }

    public String connectToGetData(Map<String, String> httpContent, Integer pageNum) {
        //每次发送请求时，替换掉page
        HttpDynColumnExecuteHandler.replacePage(httpContent, String.valueOf(pageNum));
        HttpDynColumnExecuteHandler.replacePageSize(httpContent, String.valueOf(pageSize));
        //替换offset
        HttpDynColumnExecuteHandler.replaceOffset(httpContent, String.valueOf(offset));

        Map<String, String> headerMap = JSONObject.parseObject(httpContent.get(Key.HEADER), new TypeReference<Map<String, String>>() {
        });

        Map<String, String> paramMap = JSONObject.parseObject(httpContent.get(Key.PARAM_STR), new TypeReference<Map<String, String>>() {
        });

        HttpUtils httpUtils = HttpUtils.init();
        httpUtils.setUrl(url);
        httpUtils.setHeader("Content-Type", contentType);
        httpUtils.setMethod(requestType);

        httpUtils.setHeaderMap(headerMap);
        httpUtils.setParamMap(paramMap);
        httpUtils.setBody(this.bodyStr);
        String resultStr;
        try {
            resultStr = RetryUtil.executeWithRetry(() -> {
                Map<String, String> exec = httpUtils.exec();
                if (!"200".equals(exec.get("statusCode"))) {
                    throw new RuntimeException("请求异常：" + exec.get("result"));
                }
                return exec.get("result");
            }, 3, 1000L, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        if (ReadResultTypeEnum.XML.name().equalsIgnoreCase(this.resultDataType)) {
            try {
                JSONObject result2 = XmlToJSON.toJson(resultStr);
                resultStr = JSONObject.toJSONString(result2);
            } catch (Exception e) {
                log.info(e.getMessage());
            }
        } else if (ReadResultTypeEnum.SOAP.name().equalsIgnoreCase(this.resultDataType)) {
            JSONObject result2 = SoapToJSON.toJson(resultStr);
            resultStr = JSONObject.toJSONString(result2);
        }

        //判断是否有设置返回状态码
        if (null != this.responseStatus && !responseStatus.isEmpty()) {
            FastJsonMemory resultObj = new FastJsonMemory(resultStr);
            //获取path
            Object pathValue = resultObj.get((String) responseStatus.get("path"));
            String codeValue = (String) responseStatus.get("code");
            if (null != pathValue && null != codeValue) {
                String resultCode = String.valueOf(pathValue);
                if (!resultCode.equalsIgnoreCase(codeValue)) {
                    throw AggregationException.asException(HttpReaderErrorCode.RESPONSE_STATUS_ERROR, String.format("业务成功状态码：[%s],请求返回状态码：[%S]", codeValue, resultCode));
                }
            } else {
                throw AggregationException.asException(HttpReaderErrorCode.REQUIRED_VALUE, "状态码path和code为不能为空！");
            }
        }

        return resultStr;
    }

    private Integer sendAnalysisDataToWriter(FastJsonMemory resultJson, RecordSender recordSender) {
        // 解析json为record
        JSONArray arrayResult;
        try {
            arrayResult = UnstructuredStorageReaderUtil.detailJson(resultJson, this.columnEntries);
        } catch (Exception e) {
            log.info("采集出错，采集结果：{}", resultJson);
            throw new RuntimeException(e);
        }
        //判断是主节点为对象还是数组
        for (int i = 0; i < arrayResult.size(); i++) {
            JSONObject jsonObject = arrayResult.getJSONObject(i);
            Record record = UnstructuredStorageReaderUtil.httpReadJsonDataOneRecord(jsonObject, recordSender, this.columnEntries);
            recordSender.sendToWriter(record);
//                LOG.info("pageRead读取模式数据[{}]", jsonObject);
        }
        log.info("pageRead读取模式返回结果集size:[{}]", arrayResult.size());
        if (arrayResult.size() <= 5) {
            log.info("输出数据样例:{}", resultJson);
        }
        return arrayResult.size();
    }

    private Boolean checkPage(Integer pageNum, Integer recordNum, FastJsonMemory jsonUtil) {
        //有传totalCodePath
        if (StringUtils.isNotBlank(this.totalCodePath)) {
            Object totalObj = jsonUtil.get(this.totalCodePath);
            if (null != totalObj) {
                Long total = Long.valueOf(String.valueOf(totalObj));
                //根据pageSize,total计算总页数
                Long totalPage = total % this.pageSize == 0 ? total / this.pageSize : total / this.pageSize + 1;
                return pageNum < totalPage;
            } else {
                throw AggregationException.asException(HttpReaderErrorCode.RESPONSE_PAGE_ERROR, String.format("获取返回数据总量值失败:获取路径[%s],实际返回:[%s]", this.totalCodePath, totalObj));
            }
        } else {
            //返回值，少于pageSize,返回false，分页终止
            return recordNum.equals(this.pageSize);
        }
    }

    @Override
    public void post() {

    }
}
