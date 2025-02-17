package com.jdragon.aggregation.datasource.file.ftp;

import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.FileHelper;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.net.ftp.*;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;


@Slf4j
public class FtpHelper extends AbstractPlugin implements FileHelper {

    private static final Logger LOG = LoggerFactory.getLogger(FtpHelper.class);

    private FTPClient ftpClient = null;

    private String ftpTLS;

    private String host;

    private String username;

    private String password;

    private int port;

    private String connectMode;

    private int timeout;

    @Override
    public boolean connect(Configuration configuration) {
        ftpTLS = configuration.getString("ftpTLS", Constant.DEFAULT_FTP_TLS);
        host = configuration.getNecessaryValue("host", FtpHelperErrorCode.REQUIRED_VALUE);
        username = configuration.getNecessaryValue("username", FtpHelperErrorCode.REQUIRED_VALUE);
        password = configuration.getNecessaryValue("password", FtpHelperErrorCode.REQUIRED_VALUE);
        port = configuration.getInt("port", Constant.DEFAULT_FTP_PORT);
        connectMode = configuration.getString("connectMode", Constant.DEFAULT_FTP_CONNECT_PATTERN);
        timeout = configuration.getInt("timeout", Constant.DEFAULT_TIMEOUT);

        try {
            if (StringUtils.isBlank(ftpTLS) || Objects.equals(ftpTLS, "none")) {
                this.ftpClient = new FTPClient();
                this.login(host, username, password, port, timeout, connectMode);
            } else if (Objects.equals(ftpTLS, "implicit")) {
                this.ftpClient = new FTPSClient("SSL", true);
                this.login(host, username, password, port, timeout, connectMode);
            } else if (Objects.equals(ftpTLS, "explicit")) {
                this.ftpClient = new FTPSClient("SSL");
                this.login(host, username, password, port, timeout, connectMode);
            } else {
                throw new AggregationException(FtpHelperErrorCode.FTP_TLS_EXCEPTION, "不支持的tls模式" + ftpTLS);
            }
            int reply = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(reply)) {
                ftpClient.disconnect();
                String message = String.format("与ftp服务器建立连接失败,请检查用户名和密码是否正确: [%s]",
                        "message:host =" + host + ",username = " + username + ",port =" + port);
                LOG.error(message);
                throw AggregationException.asException(FtpHelperErrorCode.FAIL_LOGIN, message);
            }
            if (this.ftpClient instanceof FTPSClient) {
                ((FTPSClient) ftpClient).execPBSZ(0);
                ((FTPSClient) ftpClient).execPROT("P");
                ftpClient.type(FTP.BINARY_FILE_TYPE);
            }
            //设置命令传输编码
            String fileEncoding = Charset.defaultCharset().displayName();
            ftpClient.setControlEncoding(fileEncoding);
        } catch (UnknownHostException e) {
            String message = String.format("请确认ftp服务器地址是否正确，无法连接到地址为: [%s] 的ftp服务器", host);
            LOG.error(message);
            throw AggregationException.asException(FtpHelperErrorCode.FAIL_LOGIN, message, e);
        } catch (IllegalArgumentException e) {
            String message = String.format("请确认连接ftp服务器端口是否正确，错误的端口: [%s] ", port);
            LOG.error(message);
            throw AggregationException.asException(FtpHelperErrorCode.FAIL_LOGIN, message, e);
        } catch (Exception e) {
            String message = String.format("与ftp服务器建立连接失败 : [%s]",
                    "message:host =" + host + ",username = " + username + ",port =" + port);
            LOG.error(message);
            throw AggregationException.asException(FtpHelperErrorCode.FAIL_LOGIN, message, e);
        }
        return false;
    }

    public void login(String host, String username, String password, int port, int timeout,
                      String connectMode) throws IOException {
        ftpClient.connect(host, port);
        ftpClient.login(username, password);
        // 不需要写死ftp server的OS TYPE,FTPClient getSystemType()方法会自动识别
        // ftpClient.configure(new FTPClientConfig(FTPClientConfig.SYST_UNIX));
        ftpClient.setConnectTimeout(timeout);
        ftpClient.setDataTimeout(timeout);
        if ("PASV".equals(connectMode)) {
            ftpClient.enterRemotePassiveMode();
            ftpClient.enterLocalPassiveMode();
        } else if ("PORT".equals(connectMode)) {
            ftpClient.enterLocalActiveMode();
            // ftpClient.enterRemoteActiveMode(host, port);
        }
    }

    @Override
    public boolean exists(String path, String name) throws IOException {
        boolean isExitFlag = false;
        String filePath = processingPath(path, name);
        try {
            FTPFile[] ftpFiles = ftpClient.listFiles(new String(filePath.getBytes(), FTP.DEFAULT_CONTROL_ENCODING));
            if (ftpFiles.length == 1 && ftpFiles[0].isFile()) {
                isExitFlag = true;
            }
        } catch (IOException e) {
            String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
            LOG.error(message);
            throw AggregationException.asException(FtpHelperErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
        }
        return isExitFlag;
    }

    @Override
    public Set<String> listFile(String dir, String regex) throws IOException {
        FTPFile[] ftpFiles = this.ftpClient.listFiles(dir);
        Set<String> fileList = new HashSet<>();
        for (FTPFile fileStatus : ftpFiles) {
            String name = fileStatus.getName();
            if (name.matches(regex)) {
                fileList.add(name);
            }
        }
        return fileList;
    }

    @Override
    public boolean isFile(String dir, String fileName) throws IOException {
        FTPFile ftpFile = this.ftpClient.mlistFile(processingPath(dir, fileName));
        if (ftpFile == null) {
            return false;
        }
        return ftpFile.isFile();
    }

    @Override
    public void mkdir(String directoryPath) throws IOException {
        // ftp server不支持递归创建目录,只能一级一级创建
        StringBuilder dirPath = new StringBuilder();
        String[] dirSplit = StringUtils.split(directoryPath, "/");
        for (String dirName : dirSplit) {
            dirPath.append("/").append(dirName);
            // 如果directoryPath目录不存在,则创建
            if (ftpClient.changeWorkingDirectory(dirPath.toString())) {
                continue;
            }
            int replayCode = this.ftpClient.mkd(dirPath.toString());
            if (replayCode != FTPReply.COMMAND_OK
                    && replayCode != FTPReply.PATHNAME_CREATED) {
                log.error("create path fail [{}]", dirPath.toString());
            }
        }
    }

    @Override
    public void rm(String path) throws IOException {
        try {
            this.ftpClient.changeWorkingDirectory("/");
            this.ftpClient.deleteFile(path);
        } catch (IOException e) {
            throw AggregationException.asException(FtpHelperErrorCode.RM_FILE_EXCEPTION, e);
        }
    }


    @Override
    public boolean isConnected() {
        return ftpClient.isConnected();
    }

    @Override
    public boolean mv(String from, String to) throws Exception {
        try {
            this.ftpClient.changeWorkingDirectory("/");
            return this.ftpClient.rename(from, to);
        } catch (IOException e) {
            throw AggregationException.asException(FtpHelperErrorCode.MV_FILE_EXCEPTION, e);
        }
    }

    @Override
    public InputStream getInputStream(String path, String name) throws IOException {
        String filePath = processingPath(path, name);
        try {
            return ftpClient.retrieveFileStream(new String(filePath.getBytes(), FTP.DEFAULT_CONTROL_ENCODING));
        } catch (IOException e) {
            String message = String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filePath, filePath);
            LOG.error(message);
            throw AggregationException.asException(FtpHelperErrorCode.OPEN_FILE_ERROR, message);
        }
    }

    @Override
    public OutputStream getOutputStream(String path, String name) throws IOException {
        String filePath = processingPath(path, name);
        try {
            this.printWorkingDirectory();
            this.ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            this.ftpClient.enterLocalPassiveMode();

            boolean b = this.ftpClient.changeWorkingDirectory(path);
            this.printWorkingDirectory();
            if (!b) {
                log.error("切换目录失败.{}:{}", this.ftpClient.getReplyCode(), this.ftpClient.getReplyCode());
            }
            OutputStream writeOutputStream = this.ftpClient
                    .storeFileStream(filePath);
            LOG.info("ftp获取文件流{},状态码：{},回复：{}", filePath, this.ftpClient.getReplyCode(), this.ftpClient.getReplyString());
            String message = String.format(
                    "打开FTP文件[%s]获取写出流时出错,请确认文件%s有权限创建，有权限写出等", filePath,
                    filePath);
            if (null == writeOutputStream) {
                throw AggregationException.asException(
                        FtpHelperErrorCode.OPEN_FILE_ERROR, message);
            }

            return writeOutputStream;
        } catch (IOException e) {
            String message = String.format(
                    "写出文件 : [%s] 时出错,请确认文件:[%s]存在且配置的用户有权限写, errorMessage:%s",
                    filePath, filePath, e.getMessage());
            LOG.error(message);
            throw AggregationException.asException(
                    FtpHelperErrorCode.OPEN_FILE_ERROR, message);
        }
    }


    private void printWorkingDirectory() {
        try {
            LOG.info("current working directory:{}", this.ftpClient.printWorkingDirectory());
        } catch (Exception e) {
            LOG.warn("printWorkingDirectory error:{}", e.getMessage());
        }
    }

    @Override
    public void fresh() {
        try {
            this.ftpClient.completePendingCommand();
        } catch (IOException e) {
            throw AggregationException.asException(
                    FtpHelperErrorCode.COMPLETE_PENDING_COMMAND_ERROR, e);
        }
    }

    @Override
    public void close() throws Exception {
        if (ftpClient == null) {
            return;
        }
        if (ftpClient.isConnected()) {
            try {
                //todo ftpClient.completePendingCommand();//打开流操作之后必须，原因还需要深究
                ftpClient.logout();
            } catch (IOException e) {
                String message = "与ftp服务器断开连接失败";
                LOG.error(message);
                throw AggregationException.asException(FtpHelperErrorCode.FAIL_DISCONNECT, message, e);
            } finally {
                if (ftpClient.isConnected()) {
                    try {
                        ftpClient.disconnect();
                    } catch (IOException e) {
                        String message = "与ftp服务器断开连接失败";
                        LOG.error(message);
                    }
                }

            }
        }
    }
}
