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


public class HttpTest {

    private final static Logger log = LoggerFactory.getLogger(HttpTest.class);

    public static void main(String[] args) {
        Configuration configuration = Configuration.from(new File("C:\\dev\\ideaProject\\DataAggregation\\core\\src\\main\\resources\\http.json"));
        JobContainer container = new JobContainer(configuration);
        container.start();
    }
}
