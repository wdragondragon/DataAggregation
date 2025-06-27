package test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.jdragon.aggregation.commons.pagination.Table;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.ColumnInfo;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;

import java.util.List;
import java.util.Map;

public class InfluxTest {
    public static void main(String[] args) {
        BaseDataSourceDTO sourceDTO = new BaseDataSourceDTO();
        sourceDTO.setHost("http://172.20.10.2:8087");
//        sourceDTO.setDatabase("jdragon_org");
        sourceDTO.setDatabase("mydb");
//        sourceDTO.setBucket("test");
        sourceDTO.setUserName("admin");
        sourceDTO.setPassword("zhjl951753");
//        sourceDTO.setPassword("8FtG2nRdBqxxOtQaQfIHFCmNpsxClb9yvaqs3XbHmMlEbeSZdNlz69MSK7rORbKYEguGE82pBgHJlkQeRi4CJw==");
        sourceDTO.setType("influxdbv1");
        try (PluginClassLoaderCloseable loaderSwapper =
                     PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, "influxdbv1")) {
            AbstractDataSourcePlugin sourcePlugin = loaderSwapper.loadPlugin();
            List<String> tables = sourcePlugin.getTableNames(sourceDTO, "");
            for (String table : tables) {
                System.out.println(table);
                List<ColumnInfo> columns = sourcePlugin.getColumns(sourceDTO, table);
                for (ColumnInfo column : columns) {
                    System.out.println("column:" + column.getColumnName() + " type:" + column.getTypeName() + " index type" + column.getIndexType());
                }
                Long tableCount = sourcePlugin.getTableCount(sourceDTO, table);
                System.out.println("tableCount:" + tableCount);

                Table<Map<String, Object>> table1 = sourcePlugin.dataModelPreview(sourceDTO, table, "10");
                System.out.println(JSONObject.toJSONString(table1));
            }
        }
    }
}
