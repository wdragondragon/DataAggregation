package com.jdragon.aggregation.core.plugin;


import com.jdragon.aggregation.commons.util.Configuration;

@FunctionalInterface
public interface CustomPluginCreator {

    AbstractJobPlugin createJobPlugin(Configuration configuration, Configuration peerConfiguration);

}
