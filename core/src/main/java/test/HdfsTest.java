package test;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.datasource.file.FileHelper;
import com.jdragon.aggregation.datasource.file.LocalFileHelper;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

public class HdfsTest {
    public static void main(String[] args) {
        String path = "/user/hive/warehouse/test";
        String targetPath = "C:\\Users\\jdrag\\Desktop\\aggregation\\test";

        Configuration configuration = Configuration.newDefault();
        configuration.set("hdfsSiteFilePath", "C:\\Users\\jdrag\\Desktop\\aggregation\\core-site.xml");
        configuration.set("coreSiteFilePath", "C:\\Users\\jdrag\\Desktop\\aggregation\\hdfs-site.xml");
//        configuration.set("hdfsSiteFilePath", "http://127.0.0.1:9000/hdfs-site.xml");
//        configuration.set("coreSiteFilePath", "http://127.0.0.1:9000/core-site.xml");
//        configuration.set("haveKerberos", "false");

        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, "tbds-hdfs")) {
            FileHelper plugin = classLoaderSwapper.loadPlugin();
            plugin.connect(configuration);
            Set<String> listFile = plugin.listFile(path, ".*");
            System.out.println(listFile);
            for (String fileName : listFile) {
                try (LocalFileHelper localFileHelper = new LocalFileHelper();
                     InputStream inputStream = plugin.getInputStream(path, fileName);
                     OutputStream outputStream = localFileHelper.getOutputStream(targetPath, fileName)) {
                    IOUtils.copy(inputStream, outputStream);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
