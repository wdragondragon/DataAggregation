package com.jdragon.aggregation.plugin.httpdyn.util;


import com.alibaba.fastjson.JSON;
import com.jdragon.aggregation.plugin.httpdyn.exception.HttpException;
import com.jdragon.aggregation.plugin.httpdyn.exception.HttpRequestConfigurationException;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.SSLException;

/**
 * @Author: Jdragon
 */
public class HttpUtils {
    private static final Pattern XML_DECLARATION_ENCODING = Pattern.compile(
            "(?is)^(\\s*<\\?xml\\b[^>]*?\\bencoding\\s*=\\s*)([\"'])([^\"']+)(\\2[^>]*?\\?>)");

    private static final Logger log = LoggerFactory.getLogger(HttpUtils.class);

    private static final PoolingHttpClientConnectionManager pcm;//httpclient连接池

    private CloseableHttpClient httpClient = null; //http连接

    private final int connectionRequestTimeout = 3000;//从连接池获取连接超时时间

    private int connectTimeout = 3000;//连接超时时间

    private int socketTimeout = 3000;//获取数据超时时间

    private RequestConfig requestConfig = null;//请求配置

    private RequestConfig.Builder requestConfigBuilder = null;//build requestConfig

    private String method = "GET";

    private String url;

    private final List<NameValuePair> nameValuePairs = new ArrayList<>();

    private final List<Header> headers = new ArrayList<>();

    private HttpEntity httpEntity;

    private String requestBody;

    private final static Map<String, Class<? extends HttpRequestBase>> baseMethod = new HashMap<>();

    private final static Map<String, Class<? extends HttpEntityEnclosingRequestBase>> entityMethod = new HashMap<>();

    static {
        entityMethod.put("POST", HttpPost.class);
        entityMethod.put("PUT", HttpPut.class);
        entityMethod.put("PATCH", HttpPatch.class);
        baseMethod.put("GET", HttpGet.class);
        baseMethod.put("DELETE", HttpDelete.class);
        baseMethod.put("HEAD", HttpHead.class);
        baseMethod.put("OPTIONS", HttpOptions.class);
        baseMethod.put("TRACE", HttpTrace.class);
    }


    static {
        pcm = new PoolingHttpClientConnectionManager();
        pcm.setMaxTotal(50);//整个连接池最大连接数
        pcm.setDefaultMaxPerRoute(50);//每路由最大连接数，默认值是2
    }

    private static HttpUtils defaultInit() {
        HttpUtils httpUtils = new HttpUtils();
        if (httpUtils.requestConfig == null) {
            httpUtils.requestConfigBuilder = RequestConfig.custom().setConnectTimeout(httpUtils.connectTimeout)
                    .setConnectionRequestTimeout(httpUtils.connectionRequestTimeout)
                    .setSocketTimeout(httpUtils.socketTimeout);
            httpUtils.requestConfig = httpUtils.requestConfigBuilder.build();
        }
        return httpUtils;
    }

    /**
     * 初始化 httpUtil
     */
    public static HttpUtils init() {
        HttpUtils httpUtils = defaultInit();
        if (httpUtils.httpClient == null) {
            httpUtils.httpClient = HttpClients.custom().setConnectionManager(pcm).build();
        }
        return httpUtils;
    }

    public static HttpUtils initJson() {
        return init().setHeader("Content-type", "application/json;charset=utf-8");
    }


    public HttpUtils setMethod(String method) {
        this.method = StringUtils.defaultString(method).trim().toUpperCase(Locale.ENGLISH);
        return this;
    }

    public HttpUtils setUrl(String url) {
        this.url = url;
        return this;
    }

    /**
     * 设置请求头
     */
    public HttpUtils setHeader(String name, String value) {
        headers.removeIf(header -> header.getName().equalsIgnoreCase(name));
        Header header = new BasicHeader(name, value);
        headers.add(header);
        return this;
    }

    /**
     * 设置请求头
     */
    public HttpUtils setHeaderMap(Map<String, String> headerMap) {
        if (headerMap == null) {
            return this;
        }
        for (Map.Entry<String, String> param : headerMap.entrySet()) {
            setHeader(param.getKey(), param.getValue());
        }
        return this;
    }

    /**
     * 设置请求参数
     */
    public HttpUtils setParam(String name, String value) {
        nameValuePairs.add(new BasicNameValuePair(name, value));
        return this;
    }

    /**
     * 设置请求参数
     */
    public HttpUtils setParamMap(Map<String, String> paramMap) {
        if (paramMap == null) {
            return this;
        }
        for (Map.Entry<String, String> param : paramMap.entrySet()) {
            nameValuePairs.add(new BasicNameValuePair(param.getKey(), param.getValue()));
        }
        return this;
    }

    /**
     * 设置字符串参数
     */
    public HttpUtils setBody(Object body) {
        if (body != null) {
            String bodyStr = JSON.toJSONString(body);
            if (!StringUtils.isBlank(bodyStr)) {
                this.requestBody = bodyStr;
            }
        }
        return this;
    }

    public HttpUtils setParams(List<? extends NameValuePair> params) {
        if (params != null) {
            nameValuePairs.addAll(params);
        }
        return this;
    }

    public HttpUtils removeHeader(String name) {
        if (name != null) {
            headers.removeIf(header -> header.getName().equalsIgnoreCase(name));
        }
        return this;
    }

    public HttpUtils setRawBody(String body) {
        if (!StringUtils.isBlank(body)) {
            this.requestBody = body;
        }
        return this;
    }

//    public HttpUtils setFile(String name, MultipartFile value) {
//        if (value != null) {
//            String fileName = value.getName();
//            try {
//                MultipartEntityBuilder builder = MultipartEntityBuilder.create();
//                builder.addBinaryBody(name, value.getInputStream(), ContentType.MULTIPART_FORM_DATA, fileName);
//                this.httpEntity = builder.build();
//            } catch (IOException e) {
//                log.error(CommonConstants.DEFAULT_ERROR_MESSAGE, e);
//            }
//        }
//        return this;
//    }

    /**
     * 设置连接超时时间
     */
    public HttpUtils setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
        this.requestConfigBuilder = requestConfigBuilder.setConnectTimeout(connectTimeout);
        requestConfig = requestConfigBuilder.build();
        return this;
    }

    /**
     * 设置连接超时时间
     */
    public HttpUtils setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
        this.requestConfigBuilder = requestConfigBuilder.setSocketTimeout(socketTimeout);
        requestConfig = requestConfigBuilder.build();
        return this;
    }

    private URI getUri(String url) {
        try {
            URIBuilder uriBuilder = new URIBuilder(url);
            if (!CollectionUtils.isEmpty(nameValuePairs)) {
                List<NameValuePair> mergedParameters = new ArrayList<>(uriBuilder.getQueryParams());
                Set<String> configuredNames = new LinkedHashSet<>();
                for (NameValuePair configured : nameValuePairs) {
                    configuredNames.add(configured.getName());
                }
                mergedParameters.removeIf(existing -> configuredNames.contains(existing.getName()));
                mergedParameters.addAll(nameValuePairs);
                uriBuilder.setParameters(mergedParameters);
            }
            URI uri = uriBuilder.build();
            String scheme = StringUtils.defaultString(uri.getScheme());
            if (!("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))
                    || StringUtils.isBlank(uri.getHost())) {
                throw new HttpRequestConfigurationException("请求的url必须包含有效的 HTTP(S) 协议和主机");
            }
            return uri;
        } catch (URISyntaxException e) {
            log.error("url 地址异常 url={} exception={}", sanitizeUrlForLog(url), e.getClass().getSimpleName());
            throw new HttpRequestConfigurationException("请求的url格式不正确", e);
        }
    }

    public Map<String, String> exec(String url) {
        this.url = url;
        try {
            return exec();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, String> exec() throws ReflectiveOperationException {
        if (StringUtils.isBlank(url)) {
            throw new HttpRequestConfigurationException("请求的url不允许为空");
        }
        URI uri = this.getUri(url);
        HttpRequestBase requestBase;
        if (supportsRequestBody(method)) {
            HttpEntityEnclosingRequestBase httpRequest = entityMethod.get(method)
                    .getConstructor(URI.class)
                    .newInstance(uri);
            if (requestBody != null) {
                Charset bodyCharset = requestBodyCharset();
                httpEntity = new StringEntity(synchronizeXmlDeclaration(requestBody, bodyCharset), bodyCharset);
            }
            if (httpEntity != null) {
                httpRequest.setEntity(httpEntity);
            }
            requestBase = httpRequest;
        } else if (baseMethod.containsKey(method)) {
            requestBase = baseMethod.get(method)
                    .getConstructor(URI.class)
                    .newInstance(uri);
        } else {
            throw new HttpRequestConfigurationException("不支持的请求方法" + method);
        }
        if (!CollectionUtils.isEmpty(headers)) {
            Header[] header = new Header[headers.size()];
            requestBase.setHeaders(headers.toArray(header));
        }
        requestBase.setConfig(requestConfig);
        return exec(requestBase);
    }

    public Map<String, String> exec(HttpRequestBase requestBase) {
        Map<String, String> resultMap = new HashMap<>();
        //执行post请求
        try (CloseableHttpResponse response = httpClient.execute(requestBase)) {
            return getHttpResult(response, requestBase, resultMap);
        } catch (HttpRequestConfigurationException e) {
            requestBase.abort();
            throw e;
        } catch (SSLException | ClientProtocolException e) {
            requestBase.abort();
            log.error("HTTP 请求配置失败 url={} exception={}",
                    sanitizeUrlForLog(url), e.getClass().getSimpleName());
            throw new HttpRequestConfigurationException("HTTP 请求协议或 TLS 配置无效", e);
        } catch (IOException e) {
            requestBase.abort();
            log.error("获取http请求返回值失败 url={} exception={}",
                    sanitizeUrlForLog(url), e.getClass().getSimpleName());
            throw new HttpException("HTTP request execution failed", e);
        } catch (Exception e) {
            requestBase.abort();
            log.error("获取http请求返回值失败 url={} exception={}",
                    sanitizeUrlForLog(url), e.getClass().getSimpleName());
            throw new HttpException("HTTP request execution failed", e);
        }
    }

    private Charset requestBodyCharset() {
        String contentType = headerValue("Content-Type");
        if (StringUtils.isBlank(contentType)) {
            return StandardCharsets.UTF_8;
        }
        try {
            ContentType parsed = ContentType.parse(contentType);
            return parsed.getCharset() == null ? StandardCharsets.UTF_8 : parsed.getCharset();
        } catch (Exception e) {
            throw new HttpRequestConfigurationException("Content-Type charset 配置无效", e);
        }
    }

    private String headerValue(String name) {
        for (Header header : headers) {
            if (header.getName().equalsIgnoreCase(name)) {
                return header.getValue();
            }
        }
        return null;
    }

    private String synchronizeXmlDeclaration(String body, Charset charset) {
        if (body == null || charset == null || !isXmlRequest()) {
            return body;
        }
        Matcher matcher = XML_DECLARATION_ENCODING.matcher(body);
        if (!matcher.find()) {
            return body;
        }
        return matcher.replaceFirst(Matcher.quoteReplacement(
                matcher.group(1) + matcher.group(2) + charset.name() + matcher.group(4)));
    }

    private boolean isXmlRequest() {
        String contentType = StringUtils.defaultString(headerValue("Content-Type")).toLowerCase(Locale.ENGLISH);
        return contentType.contains("xml") || StringUtils.defaultString(requestBody).trim().startsWith("<");
    }

    /**
     * 获取请求返回值
     */
    private Map<String, String> getHttpResult(CloseableHttpResponse response, HttpUriRequest request, Map<String, String> resultMap) {

        String result = "";
        int statusCode = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            try {
                ContentType responseContentType = ContentType.get(entity);
                byte[] responseBytes = EntityUtils.toByteArray(entity);
                Charset responseCharset = resolveResponseCharset(responseContentType, responseBytes);
                result = new String(responseBytes, responseCharset);
            } catch (Exception e) {
                request.abort();
                throw new HttpException(e);
            } finally {
                EntityUtils.consumeQuietly(entity);
            }
        }

        if (!isSuccessfulStatus(statusCode)) {
            log.warn("HttpClient status code :{} request url={}", statusCode, sanitizeUrlForLog(url));
            request.abort();
        }

        resultMap.put("statusCode", statusCode + "");
        resultMap.put("result", result);
        return resultMap;
    }

    static Charset resolveResponseCharset(ContentType contentType, byte[] responseBytes) {
        if (contentType != null && contentType.getCharset() != null) {
            return contentType.getCharset();
        }
        if (responseBytes != null) {
            if (responseBytes.length >= 3
                    && (responseBytes[0] & 0xFF) == 0xEF
                    && (responseBytes[1] & 0xFF) == 0xBB
                    && (responseBytes[2] & 0xFF) == 0xBF) {
                return StandardCharsets.UTF_8;
            }
            if (responseBytes.length >= 2
                    && (responseBytes[0] & 0xFF) == 0xFE
                    && (responseBytes[1] & 0xFF) == 0xFF) {
                return StandardCharsets.UTF_16BE;
            }
            if (responseBytes.length >= 2
                    && (responseBytes[0] & 0xFF) == 0xFF
                    && (responseBytes[1] & 0xFF) == 0xFE) {
                return StandardCharsets.UTF_16LE;
            }
            int declarationLength = Math.min(responseBytes.length, 512);
            String declaration = new String(responseBytes, 0, declarationLength, StandardCharsets.ISO_8859_1);
            Matcher matcher = XML_DECLARATION_ENCODING.matcher(declaration);
            if (matcher.find()) {
                try {
                    return Charset.forName(matcher.group(3).trim());
                } catch (Exception ignored) {
                    // Invalid XML declarations fall back to the platform HTTP default below.
                }
            }
        }
        return StandardCharsets.UTF_8;
    }

    public static boolean supportsRequestBody(String method) {
        return entityMethod.containsKey(StringUtils.defaultString(method).trim().toUpperCase(Locale.ENGLISH));
    }

    static boolean isSuccessfulStatus(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    static String sanitizeUrlForLog(String rawUrl) {
        String value = StringUtils.defaultString(rawUrl).trim();
        try {
            URI uri = new URI(value);
            if (StringUtils.isNotBlank(uri.getScheme()) && StringUtils.isNotBlank(uri.getHost())) {
                return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), null, null, null).toString();
            }
        } catch (URISyntaxException ignored) {
        }
        int schemeIndex = value.indexOf("://");
        int authorityStart = schemeIndex < 0 ? 0 : schemeIndex + 3;
        int authorityEnd = value.length();
        for (char delimiter : new char[]{'/', '?', '#'}) {
            int index = value.indexOf(delimiter, authorityStart);
            if (index >= 0) {
                authorityEnd = Math.min(authorityEnd, index);
            }
        }
        String authority = value.substring(authorityStart, authorityEnd);
        int userInfoEnd = authority.lastIndexOf('@');
        if (userInfoEnd >= 0) {
            authority = authority.substring(userInfoEnd + 1);
        }
        return (schemeIndex < 0 ? "" : value.substring(0, authorityStart)) + authority;
    }
}
