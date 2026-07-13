package com.jdragon.aggregation.plugin.httpdyn.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.util.FastJsonMemory;
import com.jdragon.aggregation.plugin.httpdyn.BaseHttpDynExecutor;
import com.jdragon.aggregation.plugin.httpdyn.exception.HttpDynException;
import com.jdragon.aggregation.plugin.httpdyn.util.HttpUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.message.BasicNameValuePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Locale;
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

    private List<NameValuePair> params;

    private Map<String, String> headers;

    private Object body;


    public FromHttpToken() {
        super.setHttpDynName("dyn_from_http_token");
    }

    @Override
    public String execute(Object... paras) {
        if (paras == null || paras.length != parasNum) {
            throw new HttpDynException("参数数量异常");
        }
        for (Object para : paras) {
            if (para == null) {
                throw new HttpDynException("参数不能为空");
            }
        }
        String method = paras[0].toString();
        String url = paras[1].toString();
        String headerStr = paras[2].toString();
        String bodyStr = paras[3].toString();
        String path = paras[4].toString();

        this.init(url, headerStr, bodyStr);

        HttpUtils httpUtil = HttpUtils.initJson()
                .setUrl(requestUrl)
                .setMethod(method.toUpperCase(Locale.ENGLISH))
                .setBody(body)
                .setParams(params)
                .setHeaderMap(headers);
        try {
            Map<String, String> resultMap = httpUtil.exec();
            int statusCode = Integer.parseInt(resultMap.get("statusCode"));
            if (statusCode < 200 || statusCode >= 300) {
                throw new HttpDynException("获取token异常：HTTP status " + statusCode);
            }
            FastJsonMemory result = new FastJsonMemory(resultMap.get("result"));

            if (result.get(path) == null) {
                throw new HttpDynException("获取token异常：返回结果为空");
            }
            String token = result.get(path).toString();
            LOG.info("获取token成功");
            return token;
        } catch (HttpDynException e) {
            throw e;
        } catch (Exception e) {
            throw new HttpDynException("获取token异常：请求或响应解析失败");
        }
    }

    private void init(String url, String headerStr, String bodyStr) {
        params = new ArrayList<>();
        headers = new LinkedHashMap<>();
        parseUrl(url);
        headers.putAll(parseHeaderMap(headerStr));
        body = parseBody(bodyStr);
    }

    private void parseUrl(String url) {
        try {
            URI uri = URI.create(url);
            if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
                    || StringUtils.isBlank(uri.getRawAuthority())) {
                throw new IllegalArgumentException("invalid HTTP URL");
            }
            requestUrl = uri.getScheme() + "://" + uri.getRawAuthority()
                    + StringUtils.defaultString(uri.getRawPath());
            for (NameValuePair pair : URLEncodedUtils.parse(uri, StandardCharsets.UTF_8)) {
                params.add(new BasicNameValuePair(pair.getName(), pair.getValue()));
            }
        } catch (Exception exception) {
            throw new HttpDynException("提供的http url参数格式异常");
        }
    }

    private Map<String, String> parseHeaderMap(String value) {
        Map<String, String> result = new LinkedHashMap<>();
        if (StringUtils.isBlank(value)) {
            return result;
        }
        String text = value.trim();
        if (text.startsWith("{")) {
            try {
                JSONObject object = JSON.parseObject(text);
                for (Map.Entry<String, Object> entry : object.entrySet()) {
                    Object item = entry.getValue();
                    result.put(entry.getKey(), item instanceof Map<?, ?> || item instanceof List<?>
                            ? JSON.toJSONString(item)
                            : String.valueOf(item));
                }
                return result;
            } catch (Exception exception) {
                throw new HttpDynException("提供的http header参数格式异常");
            }
        }
        return parseLegacyKeyValueMap(text, "header");
    }

    private Object parseBody(String value) {
        if (StringUtils.isBlank(value)) {
            return new LinkedHashMap<String, Object>();
        }
        String text = value.trim();
        if (text.startsWith("{") || text.startsWith("[")) {
            try {
                return JSON.parse(text);
            } catch (Exception exception) {
                throw new HttpDynException("提供的http body参数格式异常");
            }
        }
        return new LinkedHashMap<>(parseLegacyKeyValueMap(text, "body"));
    }

    private Map<String, String> parseLegacyKeyValueMap(String text, String label) {
        Map<String, String> result = new LinkedHashMap<>();
        for (String item : splitEscaped(text, '&')) {
            List<String> keyValue = splitEscaped(item, '=');
            if (keyValue.size() < 2) {
                throw new HttpDynException("提供的http " + label + "参数格式异常");
            }
            String key = keyValue.get(0);
            StringBuilder mappedValue = new StringBuilder(keyValue.get(1));
            for (int index = 2; index < keyValue.size(); index++) {
                mappedValue.append('=').append(keyValue.get(index));
            }
            result.put(key, mappedValue.toString());
        }
        return result;
    }

    private List<String> splitEscaped(String value, char delimiter) {
        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean escaped = false;
        for (int index = 0; index < value.length(); index++) {
            char item = value.charAt(index);
            if (escaped) {
                if (item != delimiter && item != '\\') {
                    current.append('\\');
                }
                current.append(item);
                escaped = false;
                continue;
            }
            if (item == '\\') {
                escaped = true;
                continue;
            }
            if (item == delimiter) {
                result.add(current.toString());
                current.setLength(0);
            } else {
                current.append(item);
            }
        }
        if (escaped) {
            current.append('\\');
        }
        result.add(current.toString());
        return result;
    }
}
