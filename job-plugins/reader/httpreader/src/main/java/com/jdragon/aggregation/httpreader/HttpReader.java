package com.jdragon.aggregation.httpreader;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.commons.util.FastJsonMemory;
import com.jdragon.aggregation.commons.util.json.JsonExtractionResult;
import com.jdragon.aggregation.datasource.http.HttpSourceConfig;
import com.jdragon.aggregation.datasource.http.HttpSourceScanner;
import com.jdragon.aggregation.datasource.http.PageRepeatGuard;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.plugin.httpdyn.HttpDynColumnExecuteHandler;
import com.jdragon.aggregation.plugin.httpdyn.util.HttpReadTimeoutSession;
import com.jdragon.aggregation.unstructuredstorage.ColumnEntry;
import com.jdragon.aggregation.unstructuredstorage.UnstructuredStorageReaderUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class HttpReader extends Reader.Job {
    private Configuration readerSliceConfig = null;
    private HttpSourceConfig sourceConfig;
    private final HttpSourceScanner sourceScanner = new HttpSourceScanner();
    protected HttpReadTimeoutSession readTimeoutSession;

    private List<ColumnEntry> columnEntries = null;

    //请求路径
    private String url = "";
    //请求类型：post,get
    private String requestType = "";
    //响应参数类型
    private String resultDataType = "";
    //响应头信息
    private String contentType = "";
    private String protocolMode = "";
    private String soapVersion = "";
    private String soapAction = "";
    private Boolean soapFaultFail = true;

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
        this.sourceConfig = HttpSourceConfig.fromConfiguration(this.readerSliceConfig);
        this.readTimeoutSession = new HttpReadTimeoutSession(sourceConfig.getReadTimeout());

        this.url = readerSliceConfig.getNecessaryValue(Key.URL, HttpReaderErrorCode.REQUIRED_VALUE);
        this.requestType = readerSliceConfig.getNecessaryValue(Key.MODE, HttpReaderErrorCode.REQUIRED_VALUE);
        this.protocolMode = readerSliceConfig.getUnnecessaryValue(Key.PROTOCOL_MODE, "REST_JSON", null);
        this.soapVersion = readerSliceConfig.getUnnecessaryValue(Key.SOAP_VERSION, "SOAP_11", null);
        this.soapAction = readerSliceConfig.getUnnecessaryValue(Key.SOAP_ACTION, "", null);
        this.soapFaultFail = readerSliceConfig.getBool(Key.SOAP_FAULT_FAIL, true);
        this.contentType = resolveContentType();

        this.headerStr = readerSliceConfig.getString(Key.HEADER, "{}");
        this.paramStr = readerSliceConfig.getUnnecessaryValue(Key.PARAM_STR, "{}", null);
        this.bodyStr = readerSliceConfig.getString(Key.REQUEST_BODY, "{}");
        httpContent.put(Key.HEADER, headerStr);
        httpContent.put(Key.PARAM_STR, paramStr);
        httpContent.put(Key.REQUEST_BODY, bodyStr);

        this.resultDataType = readerSliceConfig.getUnnecessaryValue(Key.RESULT_DATA_TYPE, isSoapMode() ? "soap" : "json", null);


        this.totalCodePath = readerSliceConfig.getUnnecessaryValue(Key.TOTAL_CODE_PATH, null, null);
        this.responseStatus = readerSliceConfig.getMap(Key.RESPONSE_STATUS);

        this.columnEntries = UnstructuredStorageReaderUtil.getListColumnEntry(readerSliceConfig, Key.COLUMN);


        //是否分页
        this.pageSize = readerSliceConfig.getInt(Key.PAGE_SIZE, 500);
        this.pageRead = readerSliceConfig.getBool(Key.PAGE_READ, false);
    }

    @Override
    public void startRead(RecordSender recordSender) {
        sourceScanner.validatePaginationConfig(sourceConfig);
        readTimeoutSession = new HttpReadTimeoutSession(sourceConfig.getReadTimeout());
        HttpDynColumnExecuteHandler.replaceStaticVal(httpContent);
        offset = 0L;
        PageRepeatGuard pageRepeatGuard = new PageRepeatGuard(sourceConfig.getMaxRepeatedPages());

        Integer pageNum = 1;
        String resultStr = connectToGetData(new HashMap<>(httpContent), pageNum);
        if (StringUtils.isBlank(resultStr)) {
            if (StringUtils.isNotBlank(this.totalCodePath)) {
                throw AggregationException.asException(HttpReaderErrorCode.RESPONSE_PAGE_ERROR,
                        "HTTP 分页响应提前结束: page=1, 已配置总量节点但首页响应为空");
            }
            return;
        }

        FastJsonMemory resultJson = parseResponseJson(resultStr);
        Integer recordNum = sendAnalysisDataToWriter(resultJson, resultStr, recordSender, pageRepeatGuard, pageNum);
        if (this.pageRead) {
            log.info("进入pageRead读取模式");
            while (checkPage(pageNum, recordNum, resultJson)) {
                pageNum++;
                offset += pageSize;
                resultStr = connectToGetData(new HashMap<>(httpContent), pageNum);
                if (StringUtils.isBlank(resultStr)) {
                    if (StringUtils.isNotBlank(this.totalCodePath)) {
                        throw AggregationException.asException(HttpReaderErrorCode.RESPONSE_PAGE_ERROR,
                                "HTTP 分页响应提前结束: page=" + pageNum + ", 已知仍有未读取数据");
                    }
                    break;
                }
                resultJson = parseResponseJson(resultStr);
                recordNum = sendAnalysisDataToWriter(resultJson, resultStr, recordSender, pageRepeatGuard, pageNum);
            }
        }

    }

    public String connectToGetData(Map<String, String> httpContent, Integer pageNum) {
        if (sourceConfig == null) {
            throw new IllegalStateException("HTTP reader is not initialized");
        }
        return sourceScanner.connectToGetData(sourceConfig, httpContent, pageNum, offset, readTimeoutSession);
    }

    private Integer sendAnalysisDataToWriter(FastJsonMemory resultJson,
                                             String pageContent,
                                             RecordSender recordSender,
                                             PageRepeatGuard pageRepeatGuard,
                                             int pageNum) {
        // 解析json为record
        JSONArray arrayResult;
        JsonExtractionResult extracted;
        try {
            extracted = UnstructuredStorageReaderUtil.extractJson(resultJson, this.columnEntries,
                    sourceConfig.getExtractionPlan(), 0);
            arrayResult = extracted.rows();
        } catch (IllegalArgumentException e) {
            if (sourceConfig.getExtractionPlan() != null) {
                throw AggregationException.asException(HttpReaderErrorCode.RESULT_RO_JSON_ERROR,
                        "HTTP response data extraction failed: " + e.getMessage());
            }
            throw AggregationException.asException(HttpReaderErrorCode.RESULT_RO_JSON_ERROR,
                    "HTTP response data extraction failed");
        } catch (Exception e) {
            log.warn("HTTP 响应数据提取失败，异常类型：{}", e.getClass().getSimpleName());
            throw AggregationException.asException(HttpReaderErrorCode.RESULT_RO_JSON_ERROR,
                    "HTTP response data extraction failed");
        }
        if (pageRead && pageRepeatGuard.observe(pageContent)) {
            throw AggregationException.asException(HttpReaderErrorCode.RESPONSE_PAGE_ERROR,
                    "HTTP 分页检测到重复的完整页或短页，已停止读取: page=" + pageNum
                            + ", maxRepeatedPages=" + sourceConfig.getMaxRepeatedPages());
        }
        //判断是主节点为对象还是数组
        for (int i = 0; i < arrayResult.size(); i++) {
            JSONObject jsonObject = arrayResult.getJSONObject(i);
            Record record = UnstructuredStorageReaderUtil.httpReadJsonDataOneRecord(jsonObject, recordSender, this.columnEntries);
            recordSender.sendToWriter(record);
//                LOG.info("pageRead读取模式数据[{}]", jsonObject);
        }
        log.info("pageRead读取模式返回结果集size:[{}]", arrayResult.size());
        return extracted.sourceRowCount();
    }

    private FastJsonMemory parseResponseJson(String resultStr) {
        try {
            return new FastJsonMemory(resultStr);
        } catch (Exception e) {
            throw AggregationException.asException(HttpReaderErrorCode.RESULT_RO_JSON_ERROR,
                    "HTTP response JSON parse failed");
        }
    }

    private Boolean checkPage(Integer pageNum, Integer recordNum, FastJsonMemory jsonUtil) {
        //有传totalCodePath
        if (StringUtils.isNotBlank(this.totalCodePath)) {
            Object totalObj = jsonUtil.get(this.totalCodePath);
            if (null != totalObj) {
                long total;
                try {
                    total = sourceScanner.parseTotalCount(totalObj);
                } catch (IllegalArgumentException ex) {
                    throw AggregationException.asException(HttpReaderErrorCode.RESPONSE_PAGE_ERROR,
                            "HTTP 分页总量配置无效: path=" + this.totalCodePath + ", " + ex.getMessage());
                }
                //根据pageSize,total计算总页数
                long totalPage = total % this.pageSize == 0 ? total / this.pageSize : total / this.pageSize + 1;
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

    private String resolveContentType() {
        String configured = readerSliceConfig.getString(Key.CONTENT_TYPE, null);
        if (StringUtils.isNotBlank(configured)) {
            return configured;
        }
        if (!isSoapMode()) {
            return HttpConstant.getContentType();
        }
        return "SOAP_12".equalsIgnoreCase(soapVersion)
                ? "application/soap+xml;charset=UTF-8"
                : "text/xml;charset=UTF-8";
    }

    private boolean isSoapMode() {
        return "SOAP".equalsIgnoreCase(protocolMode)
                || ReadResultTypeEnum.SOAP.name().equalsIgnoreCase(readerSliceConfig.getString(Key.RESULT_DATA_TYPE, ""));
    }

}
