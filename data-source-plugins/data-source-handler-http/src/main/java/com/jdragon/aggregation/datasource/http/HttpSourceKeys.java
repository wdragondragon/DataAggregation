package com.jdragon.aggregation.datasource.http;

public final class HttpSourceKeys {
    public static final String URL = "url";
    public static final String MODE = "mode";
    public static final String PROTOCOL_MODE = "protocolMode";
    public static final String SOAP_VERSION = "soapVersion";
    public static final String SOAP_ACTION = "soapAction";
    public static final String SOAP_FAULT_FAIL = "soapFaultFail";
    public static final String CONTENT_TYPE = "contentType";
    public static final String HEADER = "header";
    public static final String PARAMS = "params";
    public static final String REQUEST_BODY = "requestBody";
    public static final String RESULT_TYPE = "resultType";
    public static final String RESPONSE_STATUS = "responseStatus";
    public static final String TOTAL_CODE_PATH = "totalCodePath";
    public static final String RECORD_PATH = "recordPath";
    public static final String PAGE_READ = "pageRead";
    public static final String PAGE_SIZE = "pageSize";
    public static final String MAX_REPEATED_PAGES = "maxRepeatedPages";
    public static final String RETRY_TIMES = "retryTimes";
    public static final String RETRY_INTERVAL_MS = "retryIntervalMs";
    public static final String RETRY_NON_IDEMPOTENT = "retryNonIdempotent";
    public static final String COLUMNS = "columns";

    public static final String STUDIO_READER_CONFIG = "__studio_http_reader_config";
    public static final String STUDIO_PUSHDOWN_FILTERS = "__studio_http_pushdown_filters";
    public static final String STUDIO_MAX_ROWS = "__studio_http_max_rows";

    private HttpSourceKeys() {
    }
}
