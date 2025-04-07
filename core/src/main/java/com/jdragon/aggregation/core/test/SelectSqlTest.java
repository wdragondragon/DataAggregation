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
import java.util.Arrays;


public class SelectSqlTest {

    private final static Logger log = LoggerFactory.getLogger(SelectSqlTest.class);

    public static void main(String[] args) {
        Configuration configuration = Configuration.newDefault();
        configuration.set("reader.type", "mysql8");
        configuration.set("reader.config.connect.host", "172.20.10.2");
        configuration.set("reader.config.connect.port", "3306");
        configuration.set("reader.config.connect.userName", "root");
        configuration.set("reader.config.connect.password", "951753");
        configuration.set("reader.config.connect.database", "agg_test");
        configuration.set("reader.config.connect.usePool", false);
        configuration.set("reader.config.selectSql", "select test1.id,test2.test_param from agg_test1 test1 join agg_test1_1 test2 on test2.id=test1.id");

        configuration.set("writer.type", "mysql8");
        configuration.set("writer.config.connect.host", "172.20.10.2");
        configuration.set("writer.config.connect.port", "3306");
        configuration.set("writer.config.connect.userName", "root");
        configuration.set("writer.config.connect.password", "951753");
        configuration.set("writer.config.connect.database", "agg_test");
        configuration.set("writer.config.connect.usePool", false);
        configuration.set("writer.config.table", "agg_test2");
        configuration.set("writer.config.columns", Arrays.asList("id", "test_param"));

        JobContainer jobContainer = new JobContainer(configuration);
        jobContainer.start();
    }
}
