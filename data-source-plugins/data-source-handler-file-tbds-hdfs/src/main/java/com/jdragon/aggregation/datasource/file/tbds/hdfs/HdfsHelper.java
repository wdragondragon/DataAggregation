package com.jdragon.aggregation.datasource.file.tbds.hdfs;

import cn.hutool.http.HttpUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.jdragon.aggregation.auth.hdfs.GetKerberosObject;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.FileHelper;

import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class HdfsHelper extends AbstractPlugin implements FileHelper {
    public static final Logger LOG = LoggerFactory.getLogger(HdfsHelper.class);

    private Configuration configuration;

    public FileSystem fileSystem = null;

    public org.apache.hadoop.conf.Configuration hadoopConf = null;

    public static final String HADOOP_SECURITY_AUTHENTICATION_KEY = "hadoop.security.authentication";
    public static final String JAVA_SECURITY_KRB5_CONF_KEY = "java.security.krb5.conf";

    private String kerberosKeytabFilePath;
    private String kerberosPrincipal;
    private String krb5Conf;

    private GetKerberosObject kerberosObject;


    public FileSystem getFileSystem() {
        try {
            if (fileSystem != null) {
                return fileSystem;
            }
            return fileSystem = kerberosObject.doAs(() -> FileSystem.get(hadoopConf));
        } catch (IOException e) {
            String message = String.format("获取FileSystem时发生网络IO异常,请检查您的网络是否正常!HDFS地址：[%s]",
                    "message");
            LOG.error(message, e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            String message = String.format("获取FileSystem失败,请检查HDFS地址是否正确: [%s]",
                    "message:defaultFS");
            LOG.error(message, e);
            throw new RuntimeException(e);
        }
    }


    @Override
    public boolean exists(String path, String name) throws IOException {
        Path p = new Path(processingPath(path, name));
        return getFileSystem().exists(p);
    }

    @Override
    public Set<String> listFile(String dir, String regex) throws IOException {
        Set<String> fileList = new HashSet<>();
        Path path = new Path(dir);
        FileSystem fileSystem = getFileSystem();
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            String name = fileStatus.getPath().getName();
            if (name.matches(regex)) {
                fileList.add(name);
            }
        }
        return fileList;
    }

    @Override
    public boolean isFile(String dir, String fileName) throws IOException {
        return getFileSystem().isFile(new Path(processingPath(dir, fileName)));
    }

    @Override
    public void mkdir(String filePath) throws IOException {
        getFileSystem().mkdirs(new Path(filePath));
    }

    @Override
    public void rm(String path) throws IOException {
        getFileSystem().delete(new Path(path), true);
    }

    @Override
    public boolean connect(Configuration configuration) {
        hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        String hdfsSiteFile = configuration.getString(Key.HDFS_SITE_FILE_PATH);

        if (StringUtils.isNotBlank(hdfsSiteFile)) {
            if (hdfsSiteFile.startsWith("http")) {
                String s = HttpUtil.get(hdfsSiteFile);
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
                hadoopConf.addResource(byteArrayInputStream);
            } else {
                if (new File(hdfsSiteFile).exists()) {
                    LOG.info("load hdfs-site.xml at {}", hdfsSiteFile);
                    hadoopConf.addResource(new Path(hdfsSiteFile));
                } else {
                    LOG.info("hdfs-site.xml {} not exist", hdfsSiteFile);
                }
            }
        } else {
            LOG.warn("hdfs-site.xml not set");
        }

        String coreSiteFile = configuration.getString(Key.CORE_SITE_FILE_PATH);

        if (StringUtils.isNotBlank(coreSiteFile)) {
            if (coreSiteFile.startsWith("http")) {
                String s = HttpUtil.get(coreSiteFile);
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
                hadoopConf.addResource(byteArrayInputStream);
            } else {
                if (new File(coreSiteFile).exists()) {
                    LOG.info("load core-site.xml at {}", coreSiteFile);
                    hadoopConf.addResource(new Path(coreSiteFile));
                } else {
                    LOG.info("core-site.xml {} not exist", coreSiteFile);
                }
            }
        } else {
            LOG.warn("core-site.xml not set");
        }

        //是否有Kerberos认证
        Boolean haveKerberos = configuration.getBool(Key.HAVE_KERBEROS, false);
        if (haveKerberos) {
            this.kerberosKeytabFilePath = configuration.getString(Key.KERBEROS_KEYTAB_FILE_PATH);
            this.kerberosPrincipal = configuration.getString(Key.KERBEROS_PRINCIPAL);
            this.krb5Conf = configuration.getString(Key.JAVA_SECURITY_KRB5_CONF_KEY);
            hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, "kerberos");
            System.setProperty(JAVA_SECURITY_KRB5_CONF_KEY, this.krb5Conf);
        }

        Configuration hadoopConfig = configuration.getConfiguration("hadoopConfig");

        if (hadoopConfig != null) {
            Map<String, String> hadoopConfigMap = JSONObject.parseObject(hadoopConfig.toJSON(), new TypeReference<Map<String, String>>() {
            });
            hadoopConfigMap.forEach(this.hadoopConf::set);
        }
        this.kerberosObject = new GetKerberosObject(kerberosPrincipal, kerberosKeytabFilePath, krb5Conf, hadoopConf, haveKerberos);
        LOG.info("hadoopConfig details:{}", JSON.toJSONString(this.hadoopConf));
        return true;
    }

    @Override
    public boolean isConnected() {
        return true;
    }

    @Override
    public boolean mv(String from, String to) throws Exception {
        Path srcPath = new Path(from);
        Path dstPath = new Path(to);
        return getFileSystem().rename(srcPath, dstPath);
    }

    @Override
    public InputStream getInputStream(String path, String name) throws IOException {
        Path p = new Path(processingPath(path, name));
        return getFileSystem().open(p);
    }

    @Override
    public OutputStream getOutputStream(String path, String name) throws IOException {
        Path p = new Path(processingPath(path, name));
        return getFileSystem().create(p);
    }

    @Override
    public void close() throws Exception {
        closeFileSystem();
    }

    //关闭FileSystem
    public void closeFileSystem() {
        try {
            FileSystem fileSystem = getFileSystem();
            fileSystem.close();
        } catch (IOException e) {
            String message = "关闭FileSystem时发生IO异常,请检查您的网络是否正常！";
            LOG.error(message);
            throw new RuntimeException(e);
        }
    }
}
