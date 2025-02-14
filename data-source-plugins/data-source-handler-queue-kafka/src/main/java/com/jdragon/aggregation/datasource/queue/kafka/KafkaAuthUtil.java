package com.jdragon.aggregation.datasource.queue.kafka;

import com.jdragon.aggregation.commons.util.Configuration;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Properties;

/**
 * @author JDragon
 * @date 2023/5/10 18:15
 * @description
 */
public class KafkaAuthUtil {
    public static void login(Properties properties, Configuration configuration) {
        Integer kerberos = configuration.getInt(KafkaAuthKey.KERBEROS);
        String principal = configuration.getString(KafkaAuthKey.PRINCIPAL);
        String username = configuration.getString(KafkaAuthKey.USERNAME);
        String password = configuration.getString(KafkaAuthKey.PASSWORD);
        if (kerberos != null && kerberos == 1) {
            String kerberosKeytabFilePath = configuration.getString(KafkaAuthKey.KERBEROS_KEYTAB_FILEPATH);
            String krb5Conf = configuration.getString(KafkaAuthKey.KRB5_CONF);
            String kerberosDomain = configuration.getString(KafkaAuthKey.KERBEROS_DOMAIN);
            try {
                login(properties, principal, krb5Conf, kerberosKeytabFilePath, kerberosDomain);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            login(properties, username, password);
        }
    }

    public static void login(Properties properties, String username, String password) {
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");
    }

    public static void login(Properties properties, String principal, String krb5ConfigPath, String keytabPath, String kerberosDomain) throws IOException {
        setKrb5Config(krb5ConfigPath);
//        setZookeeperServerPrincipal(zookeeperPrincipal);
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.kerberos.service.name", "kafka");
        properties.put("sasl.mechanism", "GSSAPI");
        properties.put("kerberos.domain.name", kerberosDomain);
        properties.put("sasl.jaas.config", getModuleContext(principal, keytabPath));
    }


    /**
     * is IBM jdk or not
     */
    private static final boolean IS_IBM_JDK = System.getProperty("java.vendor").contains("IBM");
    /**
     * line operator string
     */
    private static final String LINE_SEPARATOR = System.getProperty("line.separator");

    /**
     * IBM jdk login module
     */
    private static final String IBM_LOGIN_MODULE = "com.ibm.security.auth.module.Krb5LoginModule required";

    /**
     * oracle jdk login module
     */
    private static final String SUN_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule required";

    private static String getModuleContext(String userPrincipal, String keyTabPath) {
        StringBuilder builder = new StringBuilder();
        if (IS_IBM_JDK) {
//            builder.append(module.getName()).append(" {").append(LINE_SEPARATOR);
            builder.append(IBM_LOGIN_MODULE).append(LINE_SEPARATOR);
            builder.append("credsType=both").append(LINE_SEPARATOR);
            builder.append("principal=\"").append(userPrincipal).append("\"").append(LINE_SEPARATOR);
            builder.append("useKeytab=\"").append(keyTabPath).append("\"").append(LINE_SEPARATOR);
            //            builder.append("};").append(LINE_SEPARATOR);
        } else {
//            builder.append(module.getName()).append(" {").append(LINE_SEPARATOR);
            builder.append(SUN_LOGIN_MODULE).append(LINE_SEPARATOR);
            builder.append("useKeyTab=true").append(LINE_SEPARATOR);
            builder.append("keyTab=\"").append(keyTabPath).append("\"").append(LINE_SEPARATOR);
            builder.append("principal=\"").append(userPrincipal).append("\"").append(LINE_SEPARATOR);
            builder.append("useTicketCache=false").append(LINE_SEPARATOR);
            builder.append("storeKey=true").append(LINE_SEPARATOR);
            //            builder.append("};").append(LINE_SEPARATOR);
        }
        builder.append("debug=true;").append(LINE_SEPARATOR);

        return builder.toString();
    }


    /**
     * Zookeeper quorum principal.
     */
    public static final String ZOOKEEPER_AUTH_PRINCIPAL = "zookeeper.server.principal";

    /**
     * java security krb5 file path
     */
    public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";

    public static void setZookeeperServerPrincipal(String zkServerPrincipal)
            throws IOException {
        setSystemProperty(ZOOKEEPER_AUTH_PRINCIPAL, zkServerPrincipal);
    }


    /**
     * 设置krb5文件
     */
    public static void setKrb5Config(String krb5ConfFile)
            throws IOException {
        setSystemProperty(JAVA_SECURITY_KRB5_CONF, krb5ConfFile);
    }


    public static void setSystemProperty(String key, String value) throws IOException {
        System.setProperty(key, value);
        String ret = System.getProperty(key);
        if (ret == null) {
            throw new IOException(key + " is null.");
        }
        if (!ret.equals(value)) {
            throw new IOException(key + " is " + ret + " is not " + value + ".");
        }
    }
}
