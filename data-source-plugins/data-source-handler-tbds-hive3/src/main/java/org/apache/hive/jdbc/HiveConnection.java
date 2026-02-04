//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.hive.jdbc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.security.auth.Subject;
import javax.security.sasl.SaslException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.auth.HiveAuthUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.Utils.JdbcConnectionParams;
import org.apache.hive.service.auth.KerberosSaslHelper;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.auth.SaslQOP;
import org.apache.hive.service.cli.session.SessionUtils;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCancelDelegationTokenReq;
import org.apache.hive.service.rpc.thrift.TCancelDelegationTokenResp;
import org.apache.hive.service.rpc.thrift.TCloseSessionReq;
import org.apache.hive.service.rpc.thrift.TGetDelegationTokenReq;
import org.apache.hive.service.rpc.thrift.TGetDelegationTokenResp;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.hive.service.rpc.thrift.TProtocolVersion;
import org.apache.hive.service.rpc.thrift.TRenewDelegationTokenReq;
import org.apache.hive.service.rpc.thrift.TRenewDelegationTokenResp;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.hive.service.rpc.thrift.TSetClientInfoReq;
import org.apache.hive.service.rpc.thrift.TSetClientInfoResp;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.NoHttpResponseException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.util.PublicSuffixMatcher;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveConnection implements Connection {
    public static final Logger LOG = LoggerFactory.getLogger(HiveConnection.class.getName());
    private String jdbcUriString;
    private String host;
    private int port;
    private final Map<String, String> sessConfMap;
    private final Utils.JdbcConnectionParams connParams;
    private final boolean isEmbeddedMode;
    private TTransport transport;
    private boolean assumeSubject;
    private TCLIService.Iface client;
    private boolean isClosed = true;
    private SQLWarning warningChain = null;
    private TSessionHandle sessHandle = null;
    private final List<TProtocolVersion> supportedProtocols = new LinkedList();
    private int loginTimeout = 0;
    private TProtocolVersion protocol;
    private int fetchSize = 1000;
    private String initFile = null;
    private String wmPool = null;
    private String wmApp = null;
    private Properties clientInfo;
    private Subject loggedInSubject;

    public static List<Utils.JdbcConnectionParams> getAllUrls(String zookeeperBasedHS2Url) throws Exception {
        Utils.JdbcConnectionParams params = Utils.parseURL(zookeeperBasedHS2Url, new Properties());
        return params.getZooKeeperEnsemble() != null && !ZooKeeperHiveClientHelper.isZkHADynamicDiscoveryMode(params.getSessionVars()) ? ZooKeeperHiveClientHelper.getDirectParamsList(params) : Collections.singletonList(params);
    }

    public HiveConnection(String uri, Properties info) throws SQLException {
        try {
            this.connParams = Utils.parseURL(uri, info);
        } catch (ZooKeeperHiveClientException var9) {
            ZooKeeperHiveClientException e = var9;
            throw new SQLException(e);
        }

        this.jdbcUriString = this.connParams.getJdbcUriString();
        this.host = Utils.getCanonicalHostName(this.connParams.getHost());
        this.port = this.connParams.getPort();
        this.sessConfMap = this.connParams.getSessionVars();
        this.setupLoginTimeout();
        this.isEmbeddedMode = this.connParams.isEmbeddedMode();
        if (this.sessConfMap.containsKey("fetchSize")) {
            this.fetchSize = Integer.parseInt(this.sessConfMap.get("fetchSize"));
        }

        if (this.sessConfMap.containsKey("initFile")) {
            this.initFile = this.sessConfMap.get("initFile");
        }

        this.wmPool = this.sessConfMap.get("wmPool");
        String[] var11 = JdbcConnectionParams.APPLICATION;
        int numRetries = var11.length;

        String errMsg;
        for (int var5 = 0; var5 < numRetries; ++var5) {
            errMsg = var11[var5];
            this.wmApp = this.sessConfMap.get(errMsg);
            if (this.wmApp != null) {
                break;
            }
        }

        this.supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1);
        this.supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2);
        this.supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V3);
        this.supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V4);
        this.supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5);
        this.supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6);
        this.supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7);
        this.supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8);
        this.supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9);
        this.supportedProtocols.add(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10);
        if (this.isEmbeddedMode) {
            EmbeddedThriftBinaryCLIService embeddedClient = new EmbeddedThriftBinaryCLIService();
            embeddedClient.init(null, this.connParams.getHiveConfs());
            this.client = embeddedClient;
            this.connParams.getHiveConfs().clear();
            this.openSession();
            this.executeInitSql();
        } else {
            int maxRetries = 1;

            try {
                String strRetries = this.sessConfMap.get("retries");
                if (StringUtils.isNotBlank(strRetries)) {
                    maxRetries = Integer.parseInt(strRetries);
                }
            } catch (NumberFormatException var8) {
            }

            numRetries = 0;

            while (true) {
                try {
                    this.openTransport();
                    this.client = new TCLIService.Client(new TBinaryProtocol(this.transport));
                    this.openSession();
                    this.executeInitSql();
                    break;
                } catch (Exception var10) {
                    LOG.warn("Failed to connect to " + this.connParams.getHost() + ":" + this.connParams.getPort());
                    errMsg = null;
                    String warnMsg = "Could not open client transport with JDBC Uri: " + this.jdbcUriString + ": ";
                    if (ZooKeeperHiveClientHelper.isZkDynamicDiscoveryMode(this.sessConfMap)) {
                        errMsg = "Could not open client transport for any of the Server URI's in ZooKeeper: ";

                        while (!Utils.updateConnParamsFromZooKeeper(this.connParams)) {
                            ++numRetries;
                            if (numRetries >= maxRetries) {
                                break;
                            }

                            this.connParams.getRejectedHostZnodePaths().clear();
                        }

                        this.jdbcUriString = this.connParams.getJdbcUriString();
                        this.host = Utils.getCanonicalHostName(this.connParams.getHost());
                        this.port = this.connParams.getPort();
                    } else {
                        errMsg = warnMsg;
                        ++numRetries;
                    }

                    if (numRetries >= maxRetries) {
                        throw new SQLException(errMsg + var10.getMessage(), " 08S01", var10);
                    }

                    LOG.warn(warnMsg + var10.getMessage() + " Retrying " + numRetries + " of " + maxRetries);
                }
            }
        }

        this.client = newSynchronizedClient(this.client);
    }

    private void executeInitSql() throws SQLException {
        if (this.initFile != null) {
            try {
                List<String> sqlList = parseInitFile(this.initFile);
                Statement st = this.createStatement();
                Iterator var3 = sqlList.iterator();

                while (true) {
                    boolean hasResult;
                    do {
                        if (!var3.hasNext()) {
                            return;
                        }

                        String sql = (String) var3.next();
                        hasResult = st.execute(sql);
                    } while (!hasResult);

                    ResultSet rs = st.getResultSet();

                    while (rs.next()) {
                        System.out.println(rs.getString(1));
                    }
                }
            } catch (Exception var7) {
                Exception e = var7;
                LOG.error("Failed to execute initial SQL");
                throw new SQLException(e.getMessage());
            }
        }
    }

    public static List<String> parseInitFile(String initFile) throws IOException {
        File file = new File(initFile);
        BufferedReader br = null;
        List<String> initSqlList = null;

        try {
            FileInputStream input = new FileInputStream(file);
            br = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();

            String line;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.length() != 0 && !line.startsWith("#") && !line.startsWith("--")) {
                    line = line.concat(" ");
                    sb.append(line);
                }
            }

            initSqlList = getInitSql(sb.toString());
            return initSqlList;
        } catch (IOException var10) {
            IOException e = var10;
            LOG.error("Failed to read initial SQL file", e);
            throw new IOException(e);
        } finally {
            if (br != null) {
                br.close();
            }

        }
    }

    private static List<String> getInitSql(String sbLine) {
        char[] sqlArray = sbLine.toCharArray();
        List<String> initSqlList = new ArrayList();
        int index = 0;

        for (int beginIndex = 0; index < sqlArray.length; ++index) {
            if (sqlArray[index] == ';') {
                String sql = sbLine.substring(beginIndex, index).trim();
                initSqlList.add(sql);
                beginIndex = index + 1;
            }
        }

        return initSqlList;
    }

    private void openTransport() throws Exception {
        this.assumeSubject = "fromSubject".equals(this.sessConfMap.get("kerberosAuthType"));
        this.transport = this.isHttpTransportMode() ? this.createHttpTransport() : this.createBinaryTransport();
        if (!this.transport.isOpen()) {
            this.transport.open();
        }

        this.logZkDiscoveryMessage("Connected to " + this.connParams.getHost() + ":" + this.connParams.getPort());
    }

    public String getConnectedUrl() {
        return this.jdbcUriString;
    }

    private String getServerHttpUrl(boolean useSsl) {
        String schemeName = useSsl ? "https" : "http";
        String httpPath = this.sessConfMap.get("httpPath");
        if (httpPath == null) {
            httpPath = "/";
        } else if (!httpPath.startsWith("/")) {
            httpPath = "/" + httpPath;
        }

        return schemeName + "://" + this.host + ":" + this.port + httpPath;
    }

    private TTransport createHttpTransport() throws SQLException, TTransportException {
        boolean useSsl = this.isSslConnection();
        CloseableHttpClient httpClient = this.getHttpClient(useSsl);
        this.transport = new THttpClient(this.getServerHttpUrl(useSsl), httpClient);
        return this.transport;
    }

    private CloseableHttpClient getHttpClient(Boolean useSsl) throws SQLException {
        boolean isCookieEnabled = this.sessConfMap.get("cookieAuth") == null || !"false".equalsIgnoreCase(this.sessConfMap.get("cookieAuth"));
        String cookieName = this.sessConfMap.get("cookieName") == null ? "hive.server2.auth" : this.sessConfMap.get("cookieName");
        CookieStore cookieStore = isCookieEnabled ? new BasicCookieStore() : null;
        Map<String, String> additionalHttpHeaders = new HashMap();
        Map<String, String> customCookies = new HashMap();
        Iterator var9 = this.sessConfMap.entrySet().iterator();

        String sslTrustStorePassword;
        while (var9.hasNext()) {
            Map.Entry<String, String> entry = (Map.Entry) var9.next();
            sslTrustStorePassword = entry.getKey();
            if (sslTrustStorePassword.startsWith("http.header.")) {
                additionalHttpHeaders.put(sslTrustStorePassword.substring("http.header.".length()), entry.getValue());
            }

            if (sslTrustStorePassword.startsWith("http.cookie.")) {
                customCookies.put(sslTrustStorePassword.substring("http.cookie.".length()), entry.getValue());
            }
        }

        HttpRequestInterceptor requestInterceptor;
        String useTwoWaySSL;
        if (this.isKerberosAuthMode()) {
            if (this.assumeSubject) {
                AccessControlContext context = AccessController.getContext();
                this.loggedInSubject = Subject.getSubject(context);
                if (this.loggedInSubject == null) {
                    throw new SQLException("The Subject is not set");
                }
            }

            requestInterceptor = new HttpKerberosRequestInterceptor(this.sessConfMap.get("principal"), this.host, this.getServerHttpUrl(useSsl), this.loggedInSubject, cookieStore, cookieName, useSsl, additionalHttpHeaders, customCookies);
        } else {
            useTwoWaySSL = this.getClientDelegationToken(this.sessConfMap);
            if (useTwoWaySSL != null) {
                requestInterceptor = new HttpTokenAuthInterceptor(useTwoWaySSL, cookieStore, cookieName, useSsl, additionalHttpHeaders, customCookies);
            } else {
                requestInterceptor = new HttpBasicAuthInterceptor(this.getUserName(), this.getPassword(), cookieStore, cookieName, useSsl, additionalHttpHeaders, customCookies);
            }
        }

        HttpClientBuilder httpClientBuilder;
        if (isCookieEnabled) {
            httpClientBuilder = HttpClients.custom().setServiceUnavailableRetryStrategy(new ServiceUnavailableRetryStrategy() {
                public boolean retryRequest(HttpResponse response, int executionCount, HttpContext context) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    boolean ret = statusCode == 401 && executionCount <= 1;
                    if (ret) {
                        context.setAttribute("hive.server2.retryserver", "true");
                    }

                    return ret;
                }

                public long getRetryInterval() {
                    return 0L;
                }
            });
        } else {
            httpClientBuilder = HttpClientBuilder.create();
        }

        httpClientBuilder.setRetryHandler(new HttpRequestRetryHandler() {
            public boolean retryRequest(IOException exception, int executionCount, HttpContext context) {
                if (executionCount > 1) {
                    HiveConnection.LOG.info("Retry attempts to connect to server exceeded.");
                    return false;
                } else if (exception instanceof NoHttpResponseException) {
                    HiveConnection.LOG.info("Could not connect to the server. Retrying one more time.");
                    return true;
                } else {
                    return false;
                }
            }
        });
        httpClientBuilder.addInterceptorFirst(requestInterceptor);
        httpClientBuilder.addInterceptorLast(new XsrfHttpRequestInterceptor());

        // set the specified timeout (socketTimeout jdbc param) for http connection as well
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(loginTimeout * 1000)
                .setConnectionRequestTimeout(loginTimeout * 1000)
                .setSocketTimeout(loginTimeout * 1000).build();
        httpClientBuilder.setDefaultRequestConfig(config);

        if (useSsl) {
            useTwoWaySSL = this.sessConfMap.get("twoWay");
            String sslTrustStorePath = this.sessConfMap.get("sslTrustStore");
            sslTrustStorePassword = this.sessConfMap.get("trustStorePassword");

            try {
                SSLConnectionSocketFactory socketFactory;
                if (useTwoWaySSL != null && useTwoWaySSL.equalsIgnoreCase("true")) {
                    socketFactory = this.getTwoWaySSLSocketFactory();
                } else if (sslTrustStorePath != null && !sslTrustStorePath.isEmpty()) {
                    KeyStore sslTrustStore = KeyStore.getInstance("JKS");
                    FileInputStream fis = new FileInputStream(sslTrustStorePath);
                    Throwable var34 = null;

                    try {
                        sslTrustStore.load(fis, sslTrustStorePassword.toCharArray());
                    } catch (Throwable var26) {
                        var34 = var26;
                        throw var26;
                    } finally {
                        if (fis != null) {
                            if (var34 != null) {
                                try {
                                    fis.close();
                                } catch (Throwable var25) {
                                    var34.addSuppressed(var25);
                                }
                            } else {
                                fis.close();
                            }
                        }

                    }

                    SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(sslTrustStore, null).build();
                    socketFactory = new SSLConnectionSocketFactory(sslContext, new DefaultHostnameVerifier(null));
                } else {
                    socketFactory = SSLConnectionSocketFactory.getSocketFactory();
                }

                Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create().register("https", socketFactory).build();
//                BasicHttpClientConnectionManager basicHttpClientConnectionManager = new BasicHttpClientConnectionManager(registry);
//                SocketConfig socketConfig = SocketConfig.custom().setSoTimeout(loginTimeout * 1000).build();
//                basicHttpClientConnectionManager.setSocketConfig(socketConfig);
//                httpClientBuilder.setConnectionManager(basicHttpClientConnectionManager);
                httpClientBuilder.setConnectionManager(new BasicHttpClientConnectionManager(registry));
            } catch (Exception var28) {
                Exception e = var28;
                String msg = "Could not create an https connection to " + this.jdbcUriString + ". " + e.getMessage();
                throw new SQLException(msg, " 08S01", e);
            }
        }

        return httpClientBuilder.build();
    }

    private TTransport createUnderlyingTransport() throws TTransportException {
        TTransport transport = null;
        if (this.isSslConnection()) {
            String sslTrustStore = this.sessConfMap.get("sslTrustStore");
            String sslTrustStorePassword = this.sessConfMap.get("trustStorePassword");
            if (sslTrustStore != null && !sslTrustStore.isEmpty()) {
                transport = HiveAuthUtils.getSSLSocket(this.host, this.port, this.loginTimeout, sslTrustStore, sslTrustStorePassword);
            } else {
                transport = HiveAuthUtils.getSSLSocket(this.host, this.port, this.loginTimeout);
            }
        } else {
            transport = HiveAuthUtils.getSocketTransport(this.host, this.port, this.loginTimeout);
        }

        return transport;
    }

    private TTransport createBinaryTransport() throws SQLException, TTransportException {
        try {
            TTransport socketTransport = this.createUnderlyingTransport();
            if (!"noSasl".equals(this.sessConfMap.get("auth"))) {
                Map<String, String> saslProps = new HashMap();
                SaslQOP saslQOP = SaslQOP.AUTH;
                if (this.sessConfMap.containsKey("saslQop")) {
                    try {
                        saslQOP = SaslQOP.fromString(this.sessConfMap.get("saslQop"));
                    } catch (IllegalArgumentException var7) {
                        IllegalArgumentException e = var7;
                        throw new SQLException("Invalid saslQop parameter. " + e.getMessage(), "42000", e);
                    }

                    saslProps.put("javax.security.sasl.qop", saslQOP.toString());
                } else {
                    saslProps.put("javax.security.sasl.qop", "auth-conf,auth-int,auth");
                }

                saslProps.put("javax.security.sasl.server.authentication", "true");
                if (this.sessConfMap.containsKey("principal")) {
                    this.transport = KerberosSaslHelper.getKerberosTransport(this.sessConfMap.get("principal"), this.host, socketTransport, saslProps, this.assumeSubject);
                } else {
                    String tokenStr = this.getClientDelegationToken(this.sessConfMap);
                    if (tokenStr != null) {
                        this.transport = KerberosSaslHelper.getTokenTransport(tokenStr, this.host, socketTransport, saslProps);
                    } else {
                        String userName = this.getUserName();
                        String passwd = this.getPassword();
                        this.transport = PlainSaslHelper.getPlainTransport(userName, passwd, socketTransport);
                    }
                }
            } else {
                this.transport = socketTransport;
            }
        } catch (SaslException var8) {
            SaslException e = var8;
            throw new SQLException("Could not create secure connection to " + this.jdbcUriString + ": " + e.getMessage(), " 08S01", e);
        }

        return this.transport;
    }

    SSLConnectionSocketFactory getTwoWaySSLSocketFactory() throws SQLException {
        SSLConnectionSocketFactory socketFactory = null;

        try {
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509", "SunJSSE");
            String keyStorePath = this.sessConfMap.get("sslKeyStore");
            String keyStorePassword = this.sessConfMap.get("keyStorePassword");
            KeyStore sslKeyStore = KeyStore.getInstance("JKS");
            if (keyStorePath != null && !keyStorePath.isEmpty()) {
                FileInputStream fis = new FileInputStream(keyStorePath);
                Throwable var7 = null;

                try {
                    sslKeyStore.load(fis, keyStorePassword.toCharArray());
                } catch (Throwable var35) {
                    var7 = var35;
                    throw var35;
                } finally {
                    if (fis != null) {
                        if (var7 != null) {
                            try {
                                fis.close();
                            } catch (Throwable var33) {
                                var7.addSuppressed(var33);
                            }
                        } else {
                            fis.close();
                        }
                    }

                }

                keyManagerFactory.init(sslKeyStore, keyStorePassword.toCharArray());
                TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("SunX509");
                String trustStorePath = this.sessConfMap.get("sslTrustStore");
                String trustStorePassword = this.sessConfMap.get("trustStorePassword");
                KeyStore sslTrustStore = KeyStore.getInstance("JKS");
                if (trustStorePath != null && !trustStorePath.isEmpty()) {
                    fis = new FileInputStream(trustStorePath);
                    Throwable var11 = null;

                    try {
                        sslTrustStore.load(fis, trustStorePassword.toCharArray());
                    } catch (Throwable var34) {
                        var11 = var34;
                        throw var34;
                    } finally {
                        if (fis != null) {
                            if (var11 != null) {
                                try {
                                    fis.close();
                                } catch (Throwable var32) {
                                    var11.addSuppressed(var32);
                                }
                            } else {
                                fis.close();
                            }
                        }

                    }

                    trustManagerFactory.init(sslTrustStore);
                    SSLContext context = SSLContext.getInstance("TLS");
                    context.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), new SecureRandom());
                    socketFactory = new SSLConnectionSocketFactory(context);
                    return socketFactory;
                } else {
                    throw new IllegalArgumentException("sslTrustStore Not configured for 2 way SSL connection");
                }
            } else {
                throw new IllegalArgumentException("sslKeyStore Not configured for 2 way SSL connection, keyStorePath param is empty");
            }
        } catch (Exception var38) {
            Exception e = var38;
            throw new SQLException("Error while initializing 2 way ssl socket factory ", e);
        }
    }

    private String getClientDelegationToken(Map<String, String> jdbcConnConf) throws SQLException {
        String tokenStr = null;
        if ("delegationToken".equalsIgnoreCase(jdbcConnConf.get("auth"))) {
            try {
                tokenStr = SessionUtils.getTokenStrForm("hiveserver2ClientToken");
            } catch (IOException var4) {
                IOException e = var4;
                throw new SQLException("Error reading token ", e);
            }
        }

        return tokenStr;
    }

    private void openSession() throws SQLException {
        TOpenSessionReq openReq = new TOpenSessionReq();
        Map<String, String> openConf = new HashMap<>();
        Iterator<Map.Entry<String, String>> var3 = this.connParams.getHiveConfs().entrySet().iterator();

        Map.Entry<String, String> hiveVar;
        while (var3.hasNext()) {
            hiveVar = var3.next();
            openConf.put("set:hiveconf:" + hiveVar.getKey(), hiveVar.getValue());
        }

        var3 = this.connParams.getHiveVars().entrySet().iterator();

        while (var3.hasNext()) {
            hiveVar = var3.next();
            openConf.put("set:hivevar:" + hiveVar.getKey(), hiveVar.getValue());
        }

        openConf.put("use:database", this.connParams.getDbName());
        openConf.put("set:hiveconf:hive.server2.thrift.resultset.default.fetch.size", Integer.toString(this.fetchSize));
        if (this.wmPool != null) {
            openConf.put("set:hivevar:wmpool", this.wmPool);
        }

        if (this.wmApp != null) {
            openConf.put("set:hivevar:wmapp", this.wmApp);
        }

        Map<String, String> sessVars = this.connParams.getSessionVars();
        if (sessVars.containsKey("hive.server2.proxy.user")) {
            openConf.put("hive.server2.proxy.user", sessVars.get("hive.server2.proxy.user"));
        }

        openReq.setConfiguration(openConf);
        if ("noSasl".equals(this.sessConfMap.get("auth"))) {
            openReq.setUsername(this.sessConfMap.get("user"));
            openReq.setPassword(this.sessConfMap.get("password"));
        }

        try {
            TOpenSessionResp openResp = this.client.OpenSession(openReq);
            Utils.verifySuccess(openResp.getStatus());
            if (!this.supportedProtocols.contains(openResp.getServerProtocolVersion())) {
                throw new TException("Unsupported Hive2 protocol");
            }

            this.protocol = openResp.getServerProtocolVersion();
            this.sessHandle = openResp.getSessionHandle();
            String serverFetchSize = openResp.getConfiguration().get("hive.server2.thrift.resultset.default.fetch.size");
            if (serverFetchSize != null) {
                this.fetchSize = Integer.parseInt(serverFetchSize);
            }
        } catch (TException var6) {
            TException e = var6;
            LOG.error("Error opening session", e);
            throw new SQLException("Could not establish connection to " + this.jdbcUriString + ": " + e.getMessage(), " 08S01", e);
        }

        this.isClosed = false;
    }

    private String getUserName() {
        return this.getSessionValue("user", "anonymous");
    }

    private String getPassword() {
        return this.getSessionValue("password", "anonymous");
    }

    private boolean isSslConnection() {
        return "true".equalsIgnoreCase(this.sessConfMap.get("ssl"));
    }

    private boolean isKerberosAuthMode() {
        return !"noSasl".equals(this.sessConfMap.get("auth")) && this.sessConfMap.containsKey("principal");
    }

    private boolean isHttpTransportMode() {
        String transportMode = this.sessConfMap.get("transportMode");
        return transportMode != null && transportMode.equalsIgnoreCase("http");
    }

    private void logZkDiscoveryMessage(String message) {
        if (ZooKeeperHiveClientHelper.isZkDynamicDiscoveryMode(this.sessConfMap)) {
            LOG.info(message);
        }

    }

    private String getSessionValue(String varName, String varDefault) {
        String varValue = this.sessConfMap.get(varName);
        if (varValue == null || varValue.isEmpty()) {
            varValue = varDefault;
        }

        return varValue;
    }

    private void setupLoginTimeout() {
        String socketTimeoutStr = sessConfMap.getOrDefault("socketTimeout", String.valueOf(TimeUnit.SECONDS.toMillis(DriverManager.getLoginTimeout())));
        long timeOut = 0;
        try {
            timeOut = Long.parseLong(socketTimeoutStr);
            LOG.info("hive socketTimeout of value {}", socketTimeoutStr);
        } catch (NumberFormatException e) {
            LOG.info("Failed to parse socketTimeout of value {}", socketTimeoutStr);
        }
        if (timeOut > Integer.MAX_VALUE) {
            loginTimeout = Integer.MAX_VALUE;
        } else if (timeOut < 0) {
            loginTimeout = 0;
        } else {
            loginTimeout = (int) timeOut;
        }
    }

    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public String getDelegationToken(String owner, String renewer) throws SQLException {
        TGetDelegationTokenReq req = new TGetDelegationTokenReq(this.sessHandle, owner, renewer);

        try {
            TGetDelegationTokenResp tokenResp = this.client.GetDelegationToken(req);
            Utils.verifySuccess(tokenResp.getStatus());
            return tokenResp.getDelegationToken();
        } catch (TException var5) {
            TException e = var5;
            throw new SQLException("Could not retrieve token: " + e.getMessage(), " 08S01", e);
        }
    }

    public void cancelDelegationToken(String tokenStr) throws SQLException {
        TCancelDelegationTokenReq cancelReq = new TCancelDelegationTokenReq(this.sessHandle, tokenStr);

        try {
            TCancelDelegationTokenResp cancelResp = this.client.CancelDelegationToken(cancelReq);
            Utils.verifySuccess(cancelResp.getStatus());
        } catch (TException var4) {
            TException e = var4;
            throw new SQLException("Could not cancel token: " + e.getMessage(), " 08S01", e);
        }
    }

    public void renewDelegationToken(String tokenStr) throws SQLException {
        TRenewDelegationTokenReq cancelReq = new TRenewDelegationTokenReq(this.sessHandle, tokenStr);

        try {
            TRenewDelegationTokenResp renewResp = this.client.RenewDelegationToken(cancelReq);
            Utils.verifySuccess(renewResp.getStatus());
        } catch (TException var4) {
            TException e = var4;
            throw new SQLException("Could not renew token: " + e.getMessage(), " 08S01", e);
        }
    }

    public void clearWarnings() throws SQLException {
        this.warningChain = null;
    }

    public void close() throws SQLException {
        if (!this.isClosed) {
            TCloseSessionReq closeReq = new TCloseSessionReq(this.sessHandle);

            try {
                this.client.CloseSession(closeReq);
            } catch (TException var6) {
                TException e = var6;
                throw new SQLException("Error while cleaning up the server resources", e);
            } finally {
                this.isClosed = true;
                if (this.transport != null) {
                    this.transport.close();
                }

            }
        }

    }

    public void commit() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public Array createArrayOf(String arg0, Object[] arg1) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public Statement createStatement() throws SQLException {
        if (this.isClosed) {
            throw new SQLException("Can't create Statement, connection is closed");
        } else {
            return new HiveStatement(this, this.client, this.sessHandle, this.fetchSize);
        }
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        if (resultSetConcurrency != 1007) {
            throw new SQLException("Statement with resultset concurrency " + resultSetConcurrency + " is not supported", "HYC00");
        } else if (resultSetType == 1005) {
            throw new SQLException("Statement with resultset type " + resultSetType + " is not supported", "HYC00");
        } else {
            return new HiveStatement(this, this.client, this.sessHandle, resultSetType == 1004, this.fetchSize);
        }
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public boolean getAutoCommit() throws SQLException {
        return true;
    }

    public String getCatalog() throws SQLException {
        return "";
    }

    public Properties getClientInfo() throws SQLException {
        return this.clientInfo == null ? new Properties() : this.clientInfo;
    }

    public String getClientInfo(String name) throws SQLException {
        return this.clientInfo == null ? null : this.clientInfo.getProperty(name);
    }

    public int getHoldability() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        if (this.isClosed) {
            throw new SQLException("Connection is closed");
        } else {
            return new HiveDatabaseMetaData(this, this.client, this.sessHandle);
        }
    }

    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public String getSchema() throws SQLException {
        if (this.isClosed) {
            throw new SQLException("Connection is closed");
        } else {
            Statement stmt = this.createStatement();
            Throwable var2 = null;

            Object var5;
            try {
                ResultSet res = stmt.executeQuery("SELECT current_database()");
                Throwable var4 = null;

                try {
                    if (!res.next()) {
                        throw new SQLException("Failed to get schema information");
                    }

                    var5 = res.getString(1);
                } catch (Throwable var28) {
                    var5 = var28;
                    var4 = var28;
                    throw var28;
                } finally {
                    if (res != null) {
                        if (var4 != null) {
                            try {
                                res.close();
                            } catch (Throwable var27) {
                                var4.addSuppressed(var27);
                            }
                        } else {
                            res.close();
                        }
                    }

                }
            } catch (Throwable var30) {
                var2 = var30;
                throw var30;
            } finally {
                if (stmt != null) {
                    if (var2 != null) {
                        try {
                            stmt.close();
                        } catch (Throwable var26) {
                            var2.addSuppressed(var26);
                        }
                    } else {
                        stmt.close();
                    }
                }

            }

            return (String) var5;
        }
    }

    public int getTransactionIsolation() throws SQLException {
        return 0;
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public SQLWarning getWarnings() throws SQLException {
        return this.warningChain;
    }

    public boolean isClosed() throws SQLException {
        return this.isClosed;
    }

    public boolean isReadOnly() throws SQLException {
        return false;
    }

    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw new SQLException("timeout value was negative");
        } else {
            boolean rc = false;

            try {
                String productName = (new HiveDatabaseMetaData(this, this.client, this.sessHandle)).getDatabaseProductName();
                rc = true;
            } catch (SQLException var4) {
            }

            return rc;
        }
    }

    public String nativeSQL(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new HivePreparedStatement(this, this.client, this.sessHandle, sql);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return new HivePreparedStatement(this, this.client, this.sessHandle, sql);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return new HivePreparedStatement(this, this.client, this.sessHandle, sql);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public void rollback() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (this.isClosed) {
            throw new SQLException("Connection is closed");
        } else {
            if (!autoCommit) {
                LOG.warn("Request to set autoCommit to false; Hive does not support autoCommit=false.");
                SQLWarning warning = new SQLWarning("Hive does not support autoCommit=false");
                if (this.warningChain == null) {
                    this.warningChain = warning;
                } else {
                    this.warningChain.setNextWarning(warning);
                }
            }

        }
    }

    public void setCatalog(String catalog) throws SQLException {
        if (this.isClosed) {
            throw new SQLException("Connection is closed");
        }
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        this.clientInfo = properties;
        this.sendClientInfo();
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        if (this.clientInfo == null) {
            this.clientInfo = new Properties();
        }

        this.clientInfo.put(name, value);
        this.sendClientInfo();
    }

    private void sendClientInfo() throws SQLClientInfoException {
        TSetClientInfoReq req = new TSetClientInfoReq(this.sessHandle);
        Map<String, String> map = new HashMap();
        if (this.clientInfo != null) {
            Iterator var3 = this.clientInfo.entrySet().iterator();

            while (var3.hasNext()) {
                Map.Entry<Object, Object> e = (Map.Entry) var3.next();
                if (e.getKey() != null && e.getValue() != null) {
                    map.put(e.getKey().toString(), e.getValue().toString());
                }
            }
        }

        req.setConfiguration(map);

        try {
            TSetClientInfoResp openResp = this.client.SetClientInfo(req);
            Utils.verifySuccess(openResp.getStatus());
        } catch (SQLException | TException var5) {
            Exception e = var5;
            LOG.error("Error sending client info", e);
            throw new SQLClientInfoException("Error sending client info", null, e);
        }
    }

    public void setHoldability(int holdability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        if (this.isClosed) {
            throw new SQLException("Connection is closed");
        } else if (readOnly) {
            throw new SQLException("Enabling read-only mode not supported");
        }
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public void setSchema(String schema) throws SQLException {
        if (this.isClosed) {
            throw new SQLException("Connection is closed");
        } else if (schema != null && !schema.isEmpty()) {
            Statement stmt = this.createStatement();
            stmt.execute("use " + schema);
            stmt.close();
        } else {
            throw new SQLException("Schema name is null or empty");
        }
    }

    public void setTransactionIsolation(int level) throws SQLException {
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("Method not supported");
    }

    public TProtocolVersion getProtocol() {
        return this.protocol;
    }

    public static TCLIService.Iface newSynchronizedClient(TCLIService.Iface client) {
        return (TCLIService.Iface) Proxy.newProxyInstance(HiveConnection.class.getClassLoader(), new Class[]{TCLIService.Iface.class}, new SynchronizedHandler(client));
    }

    private static class SynchronizedHandler implements InvocationHandler {
        private final TCLIService.Iface client;
        private final ReentrantLock lock = new ReentrantLock(true);

        SynchronizedHandler(TCLIService.Iface client) {
            this.client = client;
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            Object var13;
            try {
                this.lock.lock();
                var13 = method.invoke(this.client, args);
            } catch (InvocationTargetException var9) {
                InvocationTargetException e = var9;
                if (e.getTargetException() instanceof TException) {
                    throw e.getTargetException();
                }

                throw new TException("Error in calling method " + method.getName(), e.getTargetException());
            } catch (Exception var10) {
                Exception e = var10;
                throw new TException("Error in calling method " + method.getName(), e);
            } finally {
                this.lock.unlock();
            }

            return var13;
        }
    }
}
