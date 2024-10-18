package com.jdragon.aggregation.pluginloader.constant;

import org.apache.commons.lang.StringUtils;

import java.io.File;

public class SystemConstants {
    public static String HOME = System.getProperty("aggregation.home", "C:\\Users\\jdrag\\Desktop\\aggregation");

    public static String PLUGIN_HOME = StringUtils.join(new String[]{
            HOME, "plugin"}, File.separator);

}
