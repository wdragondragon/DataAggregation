package com.jdragon.aggregation.auth.hdfs;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.PrivilegedExceptionAction;
import java.security.MessageDigest;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author JDragon
 * @date 2023/5/11 17:42
 * @description
 */
@Slf4j
public class GetKerberosObject {

    private final String principal;

    private final String userKeytabFile;

    private final String krb5Conf;

    private final Configuration configuration;

    private final Map<String, UserGroupInformation> kerberosCache = new ConcurrentHashMap<>();

    private final String authenticationType;

    public GetKerberosObject(String principal, String userKeytabFile, String krb5Conf, Configuration configuration, String authenticationType) {
        this.configuration = configuration;
        this.principal = principal;
        this.userKeytabFile = userKeytabFile;
        this.krb5Conf = krb5Conf;
        this.authenticationType = authenticationType;
    }

    public <T> T doAs(PrivilegedExceptionAction<T> privilegedExceptionAction) throws Exception {
        if (authenticationType == null || StringUtils.isBlank(authenticationType)) {
            return privilegedExceptionAction.run();
        }
        UserGroupInformation userGroupInformation = this.initHadoopSecurity(principal, userKeytabFile, krb5Conf, configuration);
        return userGroupInformation.doAs(privilegedExceptionAction);
    }

    public void doAs(PrivilegedExceptionActionNoResult privilegedExceptionAction) throws Exception {
        if (authenticationType == null || StringUtils.isBlank(authenticationType)) {
            privilegedExceptionAction.run();
            return;
        }
        doAs(() -> {
            privilegedExceptionAction.run();
            return null;
        });
    }

    public UserGroupInformation initHadoopSecurity(String kerberosPrincipal, String kerberosKeytabFilePath, String krb5Conf, Configuration conf) throws IOException {
        if ("kerberos".equalsIgnoreCase(authenticationType)) {
            String loginContextName = kerberosPrincipal + "|"
                    + fileFingerprint(kerberosKeytabFilePath) + "|" + fileFingerprint(krb5Conf);
            UserGroupInformation cached = kerberosCache.get(loginContextName);
            if (cached != null) {
                return cached;
            }
            synchronized (kerberosCache) {
                cached = kerberosCache.get(loginContextName);
                if (cached != null) return cached;
                UserGroupInformation login = LoginUtil.login(
                        kerberosPrincipal, kerberosKeytabFilePath, krb5Conf, conf);
                log.info("Kerberos login success, {}", login.toString());
                kerberosCache.put(loginContextName, login);
                return login;
            }
        }
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(null);
        return UserGroupInformation.getLoginUser();
    }

    private String fileFingerprint(String filePath) throws IOException {
        if (StringUtils.isBlank(filePath)) return "missing";
        Path path = Path.of(filePath).toAbsolutePath().normalize();
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(path)) {
                byte[] buffer = new byte[8192];
                int read;
                while ((read = input.read(buffer)) != -1) digest.update(buffer, 0, read);
            }
            StringBuilder result = new StringBuilder(64);
            for (byte value : digest.digest()) result.append(String.format("%02x", value));
            return result.toString();
        } catch (java.security.NoSuchAlgorithmException ex) {
            throw new IOException("SHA-256 is unavailable", ex);
        }
    }

    @FunctionalInterface
    public interface PrivilegedExceptionActionNoResult {
        void run() throws Exception;
    }
}
