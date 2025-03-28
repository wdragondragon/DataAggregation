package com.jdragon.aggregation.plugin.httpdyn.impl;

import com.jdragon.aggregation.commons.util.FastJsonMemory;
import com.jdragon.aggregation.plugin.httpdyn.BaseHttpDynExecutor;
import com.jdragon.aggregation.plugin.httpdyn.exception.HttpDynException;
import com.jdragon.aggregation.plugin.httpdyn.util.HttpUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author JDragon
 * @Date 2021.04.22 上午 11:29
 * @Email 1061917196@qq.com
 * @Des:
 */

public class FromHttpToken extends BaseHttpDynExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(FromHttpToken.class);

    private final int parasNum = 5;

    private String requestUrl;

    private Map<String, String> params;

    private Map<String, String> headers;

    private Map<String, String> body;


    public FromHttpToken() {
        super.setHttpDynName("dyn_from_http_token");
    }

    @Override
    public String execute(Object... paras) {
        if (!isParamIsNotBank(paras)) {
            throw new HttpDynException("参数不能为空");
        }
        String method = paras[0].toString();
        String url = paras[1].toString();
        String headerStr = paras[2].toString();
        String bodyStr = paras[3].toString();
        String path = paras[4].toString();

        if (paras.length != parasNum) {
            throw new HttpDynException("参数数量异常");
        }

        this.init(url, headerStr, bodyStr);

        HttpUtils httpUtil = HttpUtils.initJson()
                .setUrl(requestUrl)
                .setMethod(method.toUpperCase())
                .setBody(body)
                .setParamMap(params)
                .setHeaderMap(headers);
        try {
            Map<String, String> resultMap = httpUtil.exec();
            if (!resultMap.get("statusCode").equals("200")) {
                throw new Exception("获取token异常：" + resultMap.get("result"));
            }
            FastJsonMemory result = new FastJsonMemory(resultMap.get("result"));

            if (result.get(path) == null) {
                throw new Exception("获取token异常：返回结果为空");
            }
            String token = result.get(path).toString();
            LOG.info("获取token成功：{}", token);
            return token;
        } catch (Exception e) {
            throw new HttpDynException(e.getMessage());
        }
    }

    private void init(String url, String headerStr, String bodyStr) {
        requestUrl = url;
        params = new HashMap<>();
        headers = new HashMap<>();
        body = new HashMap<>();

        int splitIndex = url.indexOf("?");
        if (splitIndex != -1) {
            requestUrl = url.substring(0, splitIndex);
            String arg = url.substring(splitIndex + 1);
            String[] keyValueStrArray = arg.split("&");
            for (String keyValueStr : keyValueStrArray) {
                String[] split = keyValueStr.split("=");
                if (split.length != 2) {
                    throw new HttpDynException("提供的http url参数异常：" + keyValueStr);
                }
                params.put(split[0], split[1]);
            }
        }

        if (StringUtils.isNotBlank(headerStr)) {
            String[] headerStrArray = headerStr.split("&");
            for (String keyValueStr : headerStrArray) {
                String[] split = keyValueStr.split("=");
                if (split.length != 2) {
                    throw new HttpDynException("提供的http header参数异常：" + keyValueStr);
                }
                headers.put(split[0], split[1]);
            }
        }

        if (StringUtils.isNotBlank(bodyStr)) {
            String[] bodyArray = bodyStr.split("&");
            for (String keyValueStr : bodyArray) {
                String[] split = keyValueStr.split("=");
                if (split.length != 2) {
                    throw new HttpDynException("提供的http body参数异常：" + keyValueStr);
                }
                body.put(split[0], split[1]);
            }
        }
    }
}
