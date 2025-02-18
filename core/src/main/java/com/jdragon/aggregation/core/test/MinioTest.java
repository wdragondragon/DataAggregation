package com.jdragon.aggregation.core.test;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.datasource.file.FileHelper;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;

@Slf4j
public class MinioTest {
    public static void main(String[] args) {
        try (PluginClassLoaderCloseable loaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, "minio");
             FileHelper fileHelper = loaderSwapper.loadPlugin()) {
            Configuration configuration = Configuration.newDefault();
            configuration.set("endpoint", "http://192.168.100.194:9000");
            configuration.set("accessKey", "minioadmin");
            configuration.set("secretKey", "minioadmin");
            configuration.set("bucket", "test");
            fileHelper.connect(configuration);
            Set<String> strings = fileHelper.listFile("/test", ".*");
            log.info(strings.toString());

            OutputStream outputStream = fileHelper.getOutputStream("/test", "test.txt");
            IOUtils.write("12345", outputStream, StandardCharsets.UTF_8);
            outputStream.close();

            InputStream inputStream = fileHelper.getInputStream("/test", "test.txt");
            String fileContent = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            inputStream.close();
            log.info("file:\n{}", fileContent);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
