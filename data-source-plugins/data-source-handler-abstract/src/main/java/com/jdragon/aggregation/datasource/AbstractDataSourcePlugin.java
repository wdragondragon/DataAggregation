package com.jdragon.aggregation.datasource;

import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;

import java.sql.Connection;

public abstract class AbstractDataSourcePlugin extends AbstractPlugin {

    public abstract Connection getConnection(BaseDataSourceDTO dataSource);

}
