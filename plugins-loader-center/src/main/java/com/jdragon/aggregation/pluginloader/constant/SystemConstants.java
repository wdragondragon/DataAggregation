package com.jdragon.aggregation.pluginloader.constant;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.io.File;

@Slf4j
public class SystemConstants {
    public static String HOME = System.getProperty("aggregation.home", "C:\\Users\\jdrag\\Desktop\\aggregation");

    public static String PLUGIN_HOME = StringUtils.join(new String[]{
            HOME, "plugin"}, File.separator);

    public static String CORE_CONFIG = StringUtils.join(new String[]{
            HOME, "conf", "core.json"}, File.separator);

    static {
        log.info("加载aggregation home:{}", HOME);
        log.info("加载aggregation plugin home:{}", PLUGIN_HOME);
    }
}
