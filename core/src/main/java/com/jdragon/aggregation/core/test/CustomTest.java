package com.jdragon.aggregation.core.test;

import com.jdragon.aggregation.commons.element.LongColumn;
import com.jdragon.aggregation.commons.element.Record;
import com.jdragon.aggregation.commons.element.StringColumn;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.core.job.JobContainer;
import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.core.plugin.RecordSender;
import com.jdragon.aggregation.core.plugin.spi.Reader;
import com.jdragon.aggregation.core.transport.record.DefaultRecord;
import com.jdragon.aggregation.datasource.AbstractDataSourcePlugin;
import com.jdragon.aggregation.datasource.BaseDataSourceDTO;
import com.jdragon.aggregation.datasource.DataSourceType;
import com.jdragon.aggregation.datasource.SourcePluginType;
import com.jdragon.aggregation.pluginloader.PluginClassLoaderCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class CustomTest {

    private final static Logger log = LoggerFactory.getLogger(CustomTest.class);

    public static void main(String[] args) {
        Configuration configuration = Configuration.from(new File("C:\\dev\\ideaProject\\DataAggregation\\core\\src\\main\\resources\\custom.json"));
        configuration.merge(Configuration.from(new File("C:\\dev\\ideaProject\\DataAggregation\\core\\src\\main\\resources\\core.json")), true);
        JobContainer container = new JobContainer(configuration);
        container.addConsumerPlugin(PluginType.READER, (config, peerConfig) -> new Reader.Job() {
            private String querySql;

            private Connection connection;

            @Override
            public void init() {
                log.info("custom init");
                querySql = "select id,test_param1 from datax_test1";
                BaseDataSourceDTO baseDataSourceDTO = new BaseDataSourceDTO();
                baseDataSourceDTO.setHost("rmHost");
                baseDataSourceDTO.setPort("3305");
                baseDataSourceDTO.setDatabase("datax_test");
                baseDataSourceDTO.setUserName("root");
                baseDataSourceDTO.setPassword("951753");
                baseDataSourceDTO.setUsePool(true);
                try (PluginClassLoaderCloseable loaderSwapper =
                             PluginClassLoaderCloseable.newCurrentThreadClassLoaderSwapper(SourcePluginType.SOURCE, DataSourceType.Mysql8.getTypeName())) {
                    AbstractDataSourcePlugin dataSourcePlugin = loaderSwapper.loadPlugin();
                    connection = dataSourcePlugin.getConnection(baseDataSourceDTO);
                }
            }

            @Override
            public void startRead(RecordSender recordSender) {
                try (Statement statement = connection.createStatement();
                     ResultSet resultSet = statement.executeQuery(querySql)) {
                    while (resultSet.next()) {
                        int id = resultSet.getInt("id");
                        String testParam1 = resultSet.getString("test_param1");
                        Record record = new DefaultRecord();
                        record.setColumn(0, new LongColumn(id));
                        record.setColumn(1, new StringColumn(testParam1));
                        recordSender.sendToWriter(record);
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void post() {
                try {
                    connection.close();
                } catch (SQLException e) {
                    log.error("关闭连接失败", e);
                }
            }
        });
        container.start();
    }
}
