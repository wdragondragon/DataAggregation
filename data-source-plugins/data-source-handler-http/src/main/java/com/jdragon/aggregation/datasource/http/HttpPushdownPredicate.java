package com.jdragon.aggregation.datasource.http;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

public class HttpPushdownPredicate {
    private String field;
    private String location;
    private String requestParamName;
    private String bodyPath;
    private String headerName;
    private String pathVariable;
    private String operator;
    private List<Object> values = new ArrayList<>();
    private List<String> paginationTokens = new ArrayList<>();

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getRequestParamName() {
        return requestParamName;
    }

    public void setRequestParamName(String requestParamName) {
        this.requestParamName = requestParamName;
    }

    public String getBodyPath() {
        return bodyPath;
    }

    public void setBodyPath(String bodyPath) {
        this.bodyPath = bodyPath;
    }

    public String getHeaderName() {
        return headerName;
    }

    public void setHeaderName(String headerName) {
        this.headerName = headerName;
    }

    public String getPathVariable() {
        return pathVariable;
    }

    public void setPathVariable(String pathVariable) {
        this.pathVariable = pathVariable;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values == null ? new ArrayList<>() : new ArrayList<>(values);
    }

    public List<String> getPaginationTokens() {
        return paginationTokens;
    }

    public void setPaginationTokens(List<String> paginationTokens) {
        this.paginationTokens = paginationTokens == null ? new ArrayList<>() : new ArrayList<>(paginationTokens);
    }

    static List<HttpPushdownPredicate> parseList(String json) {
        List<HttpPushdownPredicate> result = new ArrayList<>();
        if (json == null || json.trim().isEmpty()) {
            return result;
        }
        JSONArray array = JSONArray.parseArray(json);
        for (Object item : array) {
            if (item instanceof JSONObject) {
                result.add(((JSONObject) item).toJavaObject(HttpPushdownPredicate.class));
            }
        }
        return result;
    }
}
