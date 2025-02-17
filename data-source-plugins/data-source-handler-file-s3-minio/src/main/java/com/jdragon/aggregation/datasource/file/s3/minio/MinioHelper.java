package com.jdragon.aggregation.datasource.file.s3.minio;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.FileHelper;
import io.minio.*;
import io.minio.messages.Item;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class MinioHelper implements FileHelper {
    private MinioClient minioClient;
    private String bucketName;
    private boolean connected = false;

    @Override
    public boolean connect(Configuration configuration) {
        try {
            this.minioClient = MinioClient.builder()
                    .endpoint(configuration.getString("endpoint"))
                    .credentials(configuration.getString("accessKey"),
                            configuration.getString("secretKey"))
                    .build();
            this.bucketName = configuration.getString("bucket");

            // 确保存储桶存在
            if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
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
        // MinIO 不支持直接创建目录，只能通过空文件模拟
        String emptyFile = processingPath(filePath, ".keep");
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0])) {
            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(emptyFile)
                    .stream(inputStream, 0, -1)
                    .build());
        } catch (Exception e) {
            throw new IOException("Failed to create directory: " + filePath, e);
        }
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
    }
}
