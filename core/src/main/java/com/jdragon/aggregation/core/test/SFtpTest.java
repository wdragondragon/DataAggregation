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
public class SFtpTest {
    public static void main(String[] args) {
        try (PluginClassLoaderCloseable loaderSwapper =
                     PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, "sftp")) {
            FileHelper fileHelper = loaderSwapper.loadPlugin();
            Configuration configuration = Configuration.newDefault();
            configuration.set("host", "192.168.100.194");
            configuration.set("port", "22");
            configuration.set("username", "jdragon");
            configuration.set("password", "951753");
            fileHelper.connect(configuration);
            Set<String> strings = fileHelper.listFile("/home/jdragon/dev", ".*");
            log.info(strings.toString());

            InputStream inputStream = fileHelper.getInputStream("/home/jdragon/dev", "broker.conf");
            String fileContent = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
            log.info("file:\n{}", fileContent);


            fileHelper.mkdir("/home/jdragon/dev/sftpTest");
            OutputStream outputStream = fileHelper.getOutputStream("/home/jdragon/dev/sftpTest", "test.txt");
            IOUtils.write(fileContent, outputStream, StandardCharsets.UTF_8);
            outputStream.close();

            fileHelper.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
