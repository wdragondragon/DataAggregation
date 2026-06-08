package com.jdragon.aggregation.core.test;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.datasource.file.FileHelper;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public class MinioTest {
    private static final String DEFAULT_CONFIG_PATH = "C:\\Users\\jdrag\\Desktop\\odpscmd\\conf\\odps_config.ini";

    public static void main(String[] args) {
        try (PluginClassLoaderCloseable loaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, "minio");
             FileHelper fileHelper = loaderSwapper.loadPlugin()) {
            Map<String, String> localConfig = loadLocalConfig(System.getProperty("odps.config", DEFAULT_CONFIG_PATH));
            Configuration configuration = Configuration.newDefault();
            configuration.set("endpoint", System.getProperty("minio.endpoint", "oss-cn-guangzhou.aliyuncs.com"));
            configuration.set("accessKey", required(localConfig, "access_id"));
            configuration.set("secretKey", required(localConfig, "access_key"));
            configuration.set("bucket", System.getProperty("minio.bucket", "wbsj"));
            configuration.set("storageProvider", System.getProperty("minio.storageProvider", "oss"));
            fileHelper.connect(configuration);
            Set<String> strings = fileHelper.listFile(System.getProperty("minio.path", "test"), ".*");
            log.info(strings.toString());

//            OutputStream outputStream = fileHelper.getOutputStream("/test", "test.txt");
//            IOUtils.write("12345", outputStream, StandardCharsets.UTF_8);
//            outputStream.close();
//
//            InputStream inputStream = fileHelper.getInputStream("/test", "test.txt");
//            String fileContent = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
//            inputStream.close();
//            log.info("file:\n{}", fileContent);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Map<String, String> loadLocalConfig(String path) throws Exception {
        Map<String, String> values = new LinkedHashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#") || trimmed.startsWith(";")) {
                    continue;
                }
                int index = trimmed.indexOf('=');
                if (index <= 0) {
                    continue;
                }
                values.put(trimmed.substring(0, index).trim(), trimmed.substring(index + 1).trim());
            }
        }
        return values;
    }

    private static String required(Map<String, String> config, String key) {
        String value = config.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required config key: " + key);
        }
        return value;
    }
}
