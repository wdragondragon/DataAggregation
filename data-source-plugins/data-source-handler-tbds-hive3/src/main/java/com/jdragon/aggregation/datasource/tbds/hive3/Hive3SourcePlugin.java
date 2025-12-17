package com.jdragon.aggregation.datasource.tbds.hive3;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.DataSourceType;
import com.jdragon.aggregation.datasource.rdbms.RdbmsSourcePlugin;
import com.sun.security.auth.callback.TextCallbackHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.MessageFormat;
import java.util.*;

@Slf4j
@Getter
public class Hive3SourcePlugin extends RdbmsSourcePlugin {
    private final DataSourceType type = DataSourceType.HIVE;

    private final String driver = "org.apache.hive.jdbc.HiveDriver";

    //private final String jdbc = "jdbc:hive2://%s:%s/%s";
    private final String jdbc = "jdbc:hive2://";

    private final String testQuery = "select 1";

    private final String quotationMarks = "`";

    private final String schema = "";

    private final String tableSpace = "";

    private final String separator = ";";

    @Override
    public String getExtraParameterStart() {
        return ";";
    }

    public Hive3SourcePlugin() {
        super(new HiveSourceSql());
    }

    @Override
    public String getTableSize(BaseDataSourceDTO dataSource, String table) {
        String analyzeSql = MessageFormat.format(SQLConstants.HIVE_ANALYEZE_TABLE, table);
        try {
            executeUpdate(dataSource, analyzeSql);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        String sql = MessageFormat.format(SQLConstants.HIVE_TABLE_SIZE, table);
        float dataLength = 0.0f;
        Table<Map<String, Object>> executed = executeQuerySql(dataSource, sql, true);
        for (Map<String, Object> next : executed.getBodies()) {
            if (next.get(SQLConstants.HIVE_PRPT_NAME).equals(SQLConstants.HIVE_TOTAL_SIZE)) {
                dataLength = dataLength + Float.parseFloat(next.get(SQLConstants.HIVE_PRPT_VALUE).toString());
            }
        }
        return String.valueOf(dataLength);
    }


    @Override
    public Connection getConnection(BaseDataSourceDTO dataSource) {
        String jdbcUrl = this.joinJdbcUrl(dataSource);
        dataSource.setJdbcUrl(jdbcUrl);
        dataSource.setDriverClassName(driver);
        log.info("start get {} connection, jdbcUrl={}", getType(), jdbcUrl);
        try {
            return getFiHiveConnection(dataSource);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Connection getConnection(com.jdragon.aggregation.commons.util.Configuration configuration) {
        String configurationJson = configuration.toJSON();
        JSONObject json = JSONObject.parseObject(configurationJson);

        BaseDataSourceDTO dataSource = new BaseDataSourceDTO();
        dataSource.setDriverClassName(driver);
        dataSource.setPrincipal(json.get("principal").toString());
        dataSource.setKeytabPath(json.get("keytabPath").toString());
        dataSource.setKrb5File(json.get("krb5File").toString());
        dataSource.setHost(json.get("host").toString());
        dataSource.setPort(json.get("port").toString());
        dataSource.setDatabase(json.get("database").toString());
        String jdbcUrl = this.joinJdbcUrl(dataSource);
        dataSource.setJdbcUrl(jdbcUrl);

        return getFiHiveConnection(dataSource);
    }

    @Override
    public String joinJdbcUrl(BaseDataSourceDTO dataSource) {
        StringBuilder sb = new StringBuilder();
        sb.append(getJdbc());
        String[] hostArray = dataSource.getHost().split(",");
        if (hostArray.length > 0) {
            for (String zkHost : hostArray) {
                sb.append(String.format("%s:%s,", zkHost, dataSource.getPort()));
            }
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("/");
        //hive连接可以不指定数据库，有默认数据库default
        if (StringUtils.isNotBlank(dataSource.getDatabase())) {
            sb.append(dataSource.getDatabase());
        }
        if (StringUtils.isNotEmpty(dataSource.getOther())) {
            Map<String, String> map = JSONObject.parseObject(dataSource.getOther(), new TypeReference<LinkedHashMap<String, String>>() {
            });
            if (!map.isEmpty()) {
                Set<String> keys = map.keySet();
                StringBuilder otherSb = new StringBuilder(getSeparator());
                for (String key : keys) {
                    otherSb.append(String.format("%s=%s%s", key, map.get(key), getSeparator()));
                }
                otherSb.deleteCharAt(otherSb.length() - 1);
                sb.append(otherSb);
            }
        }
        sb.append(getSeparator()).append("kerberosAuthType=fromSubject");
        return sb.toString();
    }
    /*public static Connection getFiHiveConnection(String principal, String keytabPath, String krb5File,
                                                 String jdbcUrl, String driverClassName) throws Exception {*/

    public static Connection getFiHiveConnection(BaseDataSourceDTO dataSource) {
        String principal = dataSource.getPrincipal();
        String keytabPath = dataSource.getKeytabPath();
        String krb5File = dataSource.getKrb5File();
        String jdbcUrl = dataSource.getJdbcUrl();
        String driverClassName = dataSource.getDriverClassName();

        System.setProperty("java.security.krb5.conf", krb5File);
        HashMap<String, Object> krb5Options = new HashMap<>();
        krb5Options.put("principal", principal);
        krb5Options.put("keyTab", keytabPath);
        krb5Options.put("useKeyTab", "true");
        krb5Options.put("useTicketCache", "false");
        krb5Options.put("refreshKrb5Config", "true"); //临时刷新，并发时也有可能出问题
        krb5Options.put("debug", "false");
        krb5Options.put("doNotPrompt", "true");
        krb5Options.put("storeKey", "true");
        //krb5Options.put("renewTGT", "false");
        Configuration config = new Configuration() {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
                return new AppConfigurationEntry[]{
                        new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, krb5Options)};
            }
        };

        LoginContext lc = null;
        try {
            lc = new LoginContext("SampleClient", null, new TextCallbackHandler(), config);
            lc.login();
            log.info("Kerberos login success, " + lc.getSubject());
            return Subject.doAs(lc.getSubject(), (PrivilegedExceptionAction<Connection>) () -> {
                Class.forName(driverClassName);
                return DriverManager.getConnection(jdbcUrl);
            });
        } catch (LoginException e) {
            log.error("Kerberos login fail", e);
            throw new RuntimeException(e);
        } catch (PrivilegedActionException e) {
            log.error("get connection fail", e);
            throw new RuntimeException(e);
        }

    }
}
