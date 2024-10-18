package com.jdragon.aggregation.pluginloader.constant;

import com.jdragon.aggregation.commons.spi.ErrorCode;

public enum JarLoaderErrorCode implements ErrorCode {
    PLUGIN_INIT_ERROR("Framework-1", "插件初始化错误, 该问题通常是由于引擎安装错误引起，请联系您的运维解决 ."),
    CREATE_PLUGIN_ERROR("Framework-2", "创建插件异常，请联系引擎开发团队解决"),
    PLUGIN_CAST_ERROR("Framework-3", "创建插件异常，请联系引擎开发团队解决"),
    LOAD_PLUGIN_CLASS_ERROR("Framework-4", "读取插件异常，请联系引擎开发团队解决"),
    ;


    private final String code;

    private final String description;

    JarLoaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
