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
    private static final String[] CLIENT_PROPERTY_PREFIXES = {
            "security.", "sasl.", "ssl.", "client.", "connections.",
            "request.", "retry.", "reconnect.", "metadata.", "fetch.",
            "receive.", "send.", "max.", "enable.", "auto.", "isolation.",
            "partition.", "allow.", "interceptor.", "metrics.", "metric.", "kerberos."
    };

    /**
     * Copies Kafka client connection properties from datasource metadata.
     * Topic, group, offset and poll options are task-level settings and are
     * deliberately added by the reader/writer instead.
     */
    public static void copyKafkaClientProperties(Properties properties, Configuration configuration) {
        if (properties == null || configuration == null) {
            return;
        }
        for (String key : configuration.getKeys()) {
            if (key == null || key.trim().isEmpty()) {
                continue;
            }
            String normalizedKey = key.trim();
            if (!isClientProperty(normalizedKey)) {
                continue;
            }
            Object value = literalOrPath(configuration, normalizedKey);
            if (value != null) {
                properties.put(normalizedKey, value);
            }
        }
    }

    private static boolean isClientProperty(String key) {
        if ("bootstrap.servers".equals(key)) {
            return true;
        }
        for (String prefix : CLIENT_PROPERTY_PREFIXES) {
            if (key.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static Object literalOrPath(Configuration configuration, String key) {
        Object literalValue = configuration.getMapValue("", key);
        if (literalValue != null) {
            return literalValue;
        }
        try {
            return configuration.get(key);
        } catch (RuntimeException ignored) {
            return null;
        }
    }

    public static void login(Properties properties, Configuration configuration) {
        boolean kerberos = isKerberosEnabled(configuration);
        String principal = configuration.getString(KafkaAuthKey.PRINCIPAL);
        String username = configuration.getString(KafkaAuthKey.USERNAME);
        String password = configuration.getString(KafkaAuthKey.PASSWORD);
        if (kerberos) {
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

    /**
     * Studio datasource metadata stores the kerberos flag as a JSON boolean,
     * while older job templates used 0/1. Configuration#getInt cannot parse a
     * boolean and caused all non-Kerberos jobs to fail during initialization.
     */
    private static boolean isKerberosEnabled(Configuration configuration) {
        Object value = configuration.get(KafkaAuthKey.KERBEROS);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() == 1;
        }
        if (value == null) {
            return false;
        }
        String text = String.valueOf(value).trim();
        return "1".equals(text) || Boolean.parseBoolean(text);
    }

    public static void login(Properties properties, String username, String password) {
        // Explicit Kafka security properties come from the datasource and are
        // authoritative (for example SASL_SSL + SCRAM). Generate defaults
        // only for fields that were not configured by the caller.
        properties.putIfAbsent("security.protocol", "SASL_PLAINTEXT");
        properties.putIfAbsent("sasl.mechanism", "PLAIN");
        String mechanism = String.valueOf(properties.get("sasl.mechanism")).trim();
        String normalizedMechanism = mechanism.toUpperCase(java.util.Locale.ROOT);
        String loginModule = normalizedMechanism.startsWith("SCRAM-")
                ? "org.apache.kafka.common.security.scram.ScramLoginModule"
                : ("PLAIN".equals(normalizedMechanism)
                ? "org.apache.kafka.common.security.plain.PlainLoginModule" : null);
        if (loginModule != null) {
            properties.putIfAbsent("sasl.jaas.config", loginModule + " required username=\""
                    + escapeJaasValue(username) + "\" password=\"" + escapeJaasValue(password) + "\";");
        }
    }

    private static String escapeJaasValue(String value) {
        return value == null ? "" : value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    public static void login(Properties properties, String principal, String krb5ConfigPath, String keytabPath, String kerberosDomain) throws IOException {
        if (StringUtils.isBlank(principal)) {
            throw new IllegalArgumentException("Kafka Kerberos principal must not be blank");
        }
        if (StringUtils.isBlank(krb5ConfigPath)) {
            throw new IllegalArgumentException("Kafka Kerberos krb5Conf must not be blank");
        }
        if (StringUtils.isBlank(keytabPath)) {
            throw new IllegalArgumentException("Kafka Kerberos keytab path must not be blank");
        }
        setKrb5Config(krb5ConfigPath);
//        setZookeeperServerPrincipal(zookeeperPrincipal);
        properties.putIfAbsent("security.protocol", "SASL_PLAINTEXT");
        properties.putIfAbsent("sasl.kerberos.service.name", "kafka");
        properties.putIfAbsent("sasl.mechanism", "GSSAPI");
        if (StringUtils.isNotBlank(kerberosDomain)) {
            properties.putIfAbsent("kerberos.domain.name", kerberosDomain);
        }
        properties.putIfAbsent("sasl.jaas.config", getModuleContext(principal, keytabPath));
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
