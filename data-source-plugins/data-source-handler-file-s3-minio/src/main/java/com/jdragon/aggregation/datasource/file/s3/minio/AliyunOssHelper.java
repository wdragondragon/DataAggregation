package com.jdragon.aggregation.datasource.file.s3.minio;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import com.aliyun.oss.model.*;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.FileHelper;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

@Slf4j
public class AliyunOssHelper extends AbstractPlugin implements FileHelper {
    private OSS ossClient;
    private String bucketName;
    private boolean connected = false;

    @Override
    public boolean connect(Configuration configuration) {
        try {
            String endpoint = configuration.getString("endpoint");
            String accessKeyId = configuration.getString("accessKey");
            String accessKeySecret = configuration.getString("secretKey");
            this.bucketName = configuration.getString("bucket");

            this.ossClient = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret);

            // 确保存储桶存在
            if (!ossClient.doesBucketExist(bucketName)) {
                log.info("存储桶 {} 不存在，尝试创建", bucketName);
                ossClient.createBucket(bucketName);
            }
            connected = true;
            return true;
        } catch (Exception e) {
            log.error("连接 Aliyun OSS 失败: {}", e.getMessage(), e);
            connected = false;
            return false;
        }
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public boolean exists(String path, String name) {
        try {
            if (StringUtils.isNotBlank(path)) {
                name = processingPath(path, name);
            }
            return ossClient.doesObjectExist(bucketName, name);
        } catch (Exception e) {
            log.error("检查文件 {} 是否存在失败: {}", name, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public Set<String> listFile(String dir, String regex) {
        Set<String> fileNames = new HashSet<>();
        try {
            ObjectListing objectListing = ossClient.listObjects(bucketName, dir);
            Pattern pattern = Pattern.compile(regex);

            for (OSSObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                String name = objectSummary.getKey();
                if (pattern.matcher(name).matches()) {
                    fileNames.add(name);
                }
            }
        } catch (Exception e) {
            log.error("列出目录 {} 下的文件失败: {}", dir, e.getMessage(), e);
        }
        return fileNames;
    }

    @Override
    public boolean isFile(String dir, String fileName) {
        return exists(dir, fileName);
    }

    @Override
    public void mkdir(String filePath) {
        // OSS 不支持空目录，使用占位文件来模拟目录
        try (ByteArrayInputStream emptyContent = new ByteArrayInputStream(new byte[0])) {
            ossClient.putObject(bucketName, filePath + "/", emptyContent);
        } catch (Exception e) {
            log.error("创建目录 {} 失败: {}", filePath, e.getMessage(), e);
        }
    }

    @Override
    public void rm(String path) {
        try {
            ossClient.deleteObject(bucketName, path);
        } catch (Exception e) {
            log.error("删除文件 {} 失败: {}", path, e.getMessage(), e);
        }
    }

    @Override
    public boolean mv(String from, String to) {
        try {
            // 复制文件
            CopyObjectRequest copyObjectRequest = new CopyObjectRequest(bucketName, from, bucketName, to);
            ossClient.copyObject(copyObjectRequest);
            // 删除原文件
            ossClient.deleteObject(bucketName, from);
            return true;
        } catch (Exception e) {
            log.error("移动文件失败: {} -> {}, 错误: {}", from, to, e.getMessage(), e);
            return false;
        }
    }

    @Override
    public InputStream getInputStream(String path, String name) throws IOException {
        try {
            if (StringUtils.isNotBlank(path)) {
                name = processingPath(path, name);
            }
            OSSObject ossObject = ossClient.getObject(bucketName, name);
            return ossObject.getObjectContent();
        } catch (Exception e) {
            throw new IOException("获取输入流失败: " + name, e);
        }
    }

    @Override
    public OutputStream getOutputStream(String path, String name) {
        if (StringUtils.isNotBlank(path)) {
            name = processingPath(path, name);
        }
        String finalName = name;
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                byte[] data = toByteArray();
                try (InputStream inputStream = new ByteArrayInputStream(data)) {
                    ossClient.putObject(bucketName, finalName, inputStream);
                } catch (Exception e) {
                    throw new IOException("上传文件失败: " + finalName, e);
                }
            }
        };
    }

    @Override
    public void close() {
        if (ossClient != null) {
            ossClient.shutdown();
        }
        connected = false;
    }
}
