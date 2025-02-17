package com.jdragon.aggregation.datasource.file.ftp;

import com.jdragon.aggregation.commons.spi.ErrorCode;

public enum FtpHelperErrorCode implements ErrorCode {
    REQUIRED_VALUE("Ftp-00", "您缺失了必须填写的参数值."),
    OPEN_FILE_ERROR("Ftp-06", "您配置的文件在打开时异常."),
    COMPLETE_PENDING_COMMAND_ERROR("Ftp-07", "发送completePendingCommand指令失败."),
    FAIL_LOGIN("Ftp-12", "登录失败,无法与ftp服务器建立连接."),
    FAIL_DISCONNECT("Ftp-13", "关闭ftp连接失败,无法与ftp服务器断开连接."),
    COMMAND_FTP_IO_EXCEPTION("Ftp-14", "与ftp服务器连接异常."),
    RM_FILE_EXCEPTION("Ftp-15", "您尝试删除文件出现异常."),
    MV_FILE_EXCEPTION("Ftp-16", "您尝试移动文件出现异常."),
    FTP_TLS_EXCEPTION("Ftp-20", "不支持的tls模式！")
    ;

    private final String code;
    private final String description;

    FtpHelperErrorCode(String code, String description) {
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
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }
}
