package com.jdragon.aggregation.datasource.file.s3.minio;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.FileHelper;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import io.minio.*;
import io.minio.messages.Item;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public class MinioHelper extends AbstractPlugin implements FileHelper {
    private MinioClient minioClient;
    private String bucketName;
    private boolean connected = false;

    private OkHttpClient httpClient;

    @Override
    public boolean connect(Configuration configuration) {
        try {
            this.httpClient = new OkHttpClient();
            this.minioClient = MinioClient.builder()
                    .httpClient(httpClient)
                    .endpoint(configuration.getString("endpoint"))
                    .credentials(configuration.getString("accessKey"),
                            configuration.getString("secretKey"))
                    .build();
            this.bucketName = configuration.getString("bucket");

            // 确保存储桶存在
            if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
                log.info("存储桶{}不存在，尝试创建", bucketName);
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
            }
            connected = true;
            return true;
        } catch (Exception e) {
            connected = false;
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public boolean exists(String path, String name) throws IOException {
        try {
            String objectName = processingPath(path, name);
            minioClient.statObject(StatObjectArgs.builder().bucket(bucketName).object(objectName).build());
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public Set<String> listFile(String dir, String regex) throws IOException {
        try {
            Iterable<Result<Item>> results = minioClient.listObjects(ListObjectsArgs.builder()
                    .bucket(bucketName)
                    .prefix(dir)
                    .recursive(true)
                    .build());

            return collectFileNames(results, regex);
        } catch (Exception e) {
            throw new IOException("Failed to list files in: " + dir, e);
        }
    }

    private Set<String> collectFileNames(Iterable<Result<Item>> results, String regex) throws Exception {
        Set<String> fileNames = new HashSet<>();
        for (Result<Item> result : results) {
            String name = result.get().objectName();
            if (name.matches(regex)) {
                fileNames.add(name);
            }
        }
        return fileNames;
    }

    @Override
    public boolean isFile(String dir, String fileName) throws IOException {
        return exists(dir, fileName);
    }

    @Override
    public void mkdir(String filePath) throws IOException {
        // MinIO 不支持直接创建目录
    }

    @Override
    public void rm(String path) throws IOException {
        try {
            minioClient.removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(path).build());
        } catch (Exception e) {
            throw new IOException("Failed to delete: " + path, e);
        }
    }

    @Override
    public boolean mv(String from, String to) throws Exception {
        try {
            // 先复制，再删除
            minioClient.copyObject(CopyObjectArgs.builder()
                    .bucket(bucketName)
                    .source(CopySource.builder().bucket(bucketName).object(from).build())
                    .object(to)
                    .build());
            minioClient.removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(from).build());
            return true;
        } catch (Exception e) {
            throw new IOException("Failed to move " + from + " to " + to, e);
        }
    }

    @Override
    public InputStream getInputStream(String path, String name) throws IOException {
        try {
            return minioClient.getObject(GetObjectArgs.builder()
                    .bucket(bucketName)
                    .object(processingPath(path, name))
                    .build());
        } catch (Exception e) {
            throw new IOException("Failed to get input stream for: " + processingPath(path, name), e);
        }
    }

    @Override
    public OutputStream getOutputStream(String path, String name) throws IOException {
        return new ByteArrayOutputStream() {
            @Override
            public void close() throws IOException {
                byte[] data = toByteArray();
                try (InputStream inputStream = new ByteArrayInputStream(data)) {
                    minioClient.putObject(PutObjectArgs.builder()
                            .bucket(bucketName)
                            .object(processingPath(path, name))
                            .stream(inputStream, data.length, -1)
                            .contentType("application/octet-stream")
                            .build());
                } catch (Exception e) {
                    throw new IOException("Failed to upload file: " + processingPath(path, name), e);
                }
            }
        };
    }

    @Override
    public void close() {
        // MinIO 客户端是无状态的，不需要关闭
        if (httpClient != null && connected) {
            httpClient.dispatcher().executorService().shutdown();
        }
        connected = false;
    }
}
