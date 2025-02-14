package com.jdragon.aggregation.datasource.file.sftp;

import com.jdragon.aggregation.commons.spi.ErrorCode;

public enum SftpHelperErrorCode implements ErrorCode {
    REQUIRED_VALUE("SFtp-00", "您缺失了必须填写的参数值."),
    OPEN_FILE_ERROR("SFtp-06", "您配置的文件在打开时异常."),
    FILE_NOT_EXISTS("SFtp-04", "您配置的目录文件路径不存在或者没有权限读取."),
    FAIL_LOGIN("SFtp-12", "登录失败,无法与ftp服务器建立连接."),
    FAIL_DISCONNECT("SFtp-13", "关闭ftp连接失败,无法与ftp服务器断开连接."),
    COMMAND_FTP_IO_EXCEPTION("SFtp-14", "与ftp服务器连接异常."),
    RM_FILE_EXCEPTION("SFtp-15", "您尝试删除文件出现异常."),
    MV_FILE_EXCEPTION("SFtp-16", "您尝试移动文件出现异常."),
    ;

    private final String code;
    private final String description;

    SftpHelperErrorCode(String code, String description) {
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
