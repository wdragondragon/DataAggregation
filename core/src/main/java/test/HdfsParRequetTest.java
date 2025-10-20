package test;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.datasource.file.FileHelper;
import com.jdragon.aggregation.datasource.file.LocalFileHelper;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@Slf4j
public class HdfsParRequetTest {
    public static void main(String[] args) {
        String path = "/user/hive/warehouse/test";
        String targetPath = "C:\\Users\\jdrag\\Desktop\\aggregation\\test";

        Configuration configuration = Configuration.newDefault();
        configuration.set("hdfsSiteFilePath", "C:\\Users\\jdrag\\Desktop\\aggregation\\core-site.xml");
        configuration.set("coreSiteFilePath", "C:\\Users\\jdrag\\Desktop\\aggregation\\hdfs-site.xml");

        configuration.set("sparkSessionConfigMaster", "local[*]");

        Map<String, String> sparkSessionConfig = new HashMap<>();
        sparkSessionConfig.put("spark.driver.memory", "2g");
        sparkSessionConfig.put("spark.executor.memory", "2g");
        sparkSessionConfig.put("spark.executor.cores", "2");
        sparkSessionConfig.put("spark.sql.parquet.enableVectorizedReader", "true");
        configuration.set("sparkSessionConfig", sparkSessionConfig);

        Map<String, String> sparkReadOption = new HashMap<>();
        sparkReadOption.put("mergeSchema", "true");  // 合并多个文件的schema
        sparkReadOption.put("inferSchema", "true");  // 自动推断schema
        configuration.set("sparkReadOption", sparkReadOption);

        try (PluginClassLoaderCloseable classLoaderSwapper = PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, "tbds-hdfs")) {
            FileHelper plugin = classLoaderSwapper.loadPlugin();
            plugin.connect(configuration);

            plugin.readFile("hdfs://xxxxx//parquetPath", "parquet", (Map<String, Object> rowData) -> {
                HdfsParRequetTest.log.info("插件接收到数据: {}", rowData);
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
//        Map<String, String> sparkReadOption1 = configuration.getMap("sparkSessionConfig", String.class);
//        System.out.println(sparkReadOption1);
    }
}
