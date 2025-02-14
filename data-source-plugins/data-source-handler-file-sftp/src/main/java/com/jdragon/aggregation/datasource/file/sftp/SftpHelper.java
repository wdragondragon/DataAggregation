package com.jdragon.aggregation.datasource.file.sftp;

import com.jcraft.jsch.*;
import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.FileHelper;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.*;

@Slf4j
public class SftpHelper extends AbstractPlugin implements FileHelper {

    private Session session = null;

    private ChannelSftp channelSftp = null;

    private Channel sftp = null;

    private String host;

    private String username;

    private String password;

    private int port;

    private int timeout;

    @Override
    public boolean connect(Configuration configuration) {
        host = configuration.getNecessaryValue("host", SftpHelperErrorCode.REQUIRED_VALUE);
        username = configuration.getNecessaryValue("username", SftpHelperErrorCode.REQUIRED_VALUE);
        password = configuration.getNecessaryValue("password", SftpHelperErrorCode.REQUIRED_VALUE);
        port = configuration.getInt("port", Constant.DEFAULT_FTP_PORT);
        timeout = configuration.getInt("timeout", Constant.DEFAULT_TIMEOUT);
        JSch jsch = new JSch(); // 创建JSch对象
        try {
            session = jsch.getSession(username, host, port);
            // 根据用户名，主机ip，端口获取一个Session对象
            // 如果服务器连接不上，则抛出异常
            if (session == null) {
                throw AggregationException.asException(SftpHelperErrorCode.FAIL_LOGIN,
                        "session is null,无法通过sftp与服务器建立链接，请检查主机名和用户名是否正确.");
            }

            session.setPassword(password); // 设置密码
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config); // 为Session对象设置properties
            session.setTimeout(timeout); // 设置timeout时间
            session.connect(); // 通过Session建立链接
            channelSftp = (ChannelSftp) session.openChannel("sftp"); // 打开SFTP通道
            channelSftp.connect(); // 建立SFTP通道的连接
            sftp = session.openChannel("exec");
            //设置命令传输编码
            //String fileEncoding = System.getProperty("file.encoding");
            //channelSftp.setFilenameEncoding(fileEncoding);
        } catch (JSchException e) {
            if (null != e.getCause()) {
                String cause = e.getCause().toString();
                String unknownHostException = "java.net.UnknownHostException: " + host;
                String illegalArgumentException = "java.lang.IllegalArgumentException: port out of range:" + port;
                String wrongPort = "java.net.ConnectException: Connection refused";
                if (unknownHostException.equals(cause)) {
                    String message = String.format("请确认ftp服务器地址是否正确，无法连接到地址为: [%s] 的ftp服务器", host);
                    log.error(message);
                    throw AggregationException.asException(SftpHelperErrorCode.FAIL_LOGIN, message, e);
                } else if (illegalArgumentException.equals(cause) || wrongPort.equals(cause)) {
                    String message = String.format("请确认连接ftp服务器端口是否正确，错误的端口: [%s] ", port);
                    log.error(message);
                    throw AggregationException.asException(SftpHelperErrorCode.FAIL_LOGIN, message, e);
                }
            } else {
                if ("Auth fail".equals(e.getMessage())) {
                    String message = String.format("与ftp服务器建立连接失败,请检查用户名和密码是否正确: [%s]",
                            "message:host =" + host + ",username = " + username + ",port =" + port);
                    log.error(message);
                    throw AggregationException.asException(SftpHelperErrorCode.FAIL_LOGIN, message);
                } else {
                    String message = String.format("与ftp服务器建立连接失败 : [%s]",
                            "message:host =" + host + ",username = " + username + ",port =" + port);
                    log.error(message);
                    throw AggregationException.asException(SftpHelperErrorCode.FAIL_LOGIN, message, e);
                }
            }
        }
        return false;
    }


    @Override
    public boolean exists(String path, String name) throws IOException {
        String filePath = processingPath(path, name);
        boolean isExitFlag = false;
        try {
            SftpATTRS sftpATTRS = channelSftp.lstat(filePath);
            if (sftpATTRS.getSize() >= 0) {
                isExitFlag = true;
            }
        } catch (SftpException e) {
            if (e.getMessage().equalsIgnoreCase("no such file")) {
                String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", filePath);
                log.error(message);
                throw AggregationException.asException(SftpHelperErrorCode.FILE_NOT_EXISTS, message);
            } else {
                String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
                log.error(message);
                throw AggregationException.asException(SftpHelperErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
            }
        }
        return isExitFlag;
    }

    @Override
    public Set<String> listFile(String directoryPath, String regex) throws IOException {
        Set<String> fileList = new HashSet<>();
        try {
            Vector<ChannelSftp.LsEntry> vector = (Vector<ChannelSftp.LsEntry>) this.channelSftp.ls(directoryPath);
            for (ChannelSftp.LsEntry le : vector) {
                String name = le.getFilename();
//                String filePath = directoryPath + name;
                if (name.matches(regex)) {
                    fileList.add(name);
                }
            }
            return fileList;
        } catch (SftpException e) {
            String message = String.format("获取path：[%s] 下文件列表时发生I/O异常,请确认与ftp服务器的连接正常", directoryPath);
            log.error(message);
            throw AggregationException.asException(SftpHelperErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
        }
    }

    @Override
    public boolean isFile(String dir, String fileName) throws IOException {
        return isFileExist(processingPath(dir, fileName));
    }

    @Override
    public void mkdir(String directoryPath) throws IOException {
        boolean isDirExist = false;
        try {
            this.printWorkingDirectory();
            SftpATTRS sftpATTRS = this.channelSftp.lstat(directoryPath);
            isDirExist = sftpATTRS.isDir();
        } catch (SftpException e) {
            if (e.getMessage().equalsIgnoreCase("no such file")) {
                log.warn("您的配置项path:[{}]不存在，将尝试进行目录创建, errorMessage:{}", directoryPath, e.getMessage());
            } else {
                log.error(e.getMessage(), e);
            }
        }
        if (!isDirExist) {
            StringBuilder dirPath = new StringBuilder();
            dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
            String[] dirSplit = StringUtils.split(directoryPath, IOUtils.DIR_SEPARATOR_UNIX);
            try {
                // ftp server不支持递归创建目录,只能一级一级创建
                for (String dirName : dirSplit) {
                    dirPath.append(dirName);
                    mkDirSingleHierarchy(dirPath.toString());
                    dirPath.append(IOUtils.DIR_SEPARATOR_UNIX);
                }
            } catch (SftpException e) {
                String message = String
                        .format("创建目录:%s时发生I/O异常,请确认与ftp服务器的连接正常,拥有目录创建权限, errorMessage:%s",
                                directoryPath, e.getMessage());
                log.error(message, e);
                throw AggregationException
                        .asException(
                                SftpHelperErrorCode.COMMAND_FTP_IO_EXCEPTION,
                                message, e);
            }
        }
    }

    public boolean mkDirSingleHierarchy(String directoryPath) throws SftpException {
        boolean isDirExist = false;
        try {
            SftpATTRS sftpATTRS = this.channelSftp.lstat(directoryPath);
            isDirExist = sftpATTRS.isDir();
        } catch (SftpException e) {
            log.info("正在逐级创建目录 [{}]", directoryPath);
            this.channelSftp.mkdir(directoryPath);
            return true;
        }
        if (!isDirExist) {
            log.info("正在逐级创建目录 [{}]", directoryPath);
            this.channelSftp.mkdir(directoryPath);
        }
        return true;
    }

    @Override
    public void rm(String path) throws IOException {
        try {
            if (isFileExist(path)) {
                channelSftp.rm(path);
            }
        } catch (SftpException e) {
            String message = String.format("rm文件出错 : [%s] 时出错", path);
            throw AggregationException.asException(SftpHelperErrorCode.RM_FILE_EXCEPTION, message, e);
        }
    }


    @Override
    public boolean isConnected() {
        return channelSftp.isConnected();
    }

    @Override
    public boolean mv(String source, String target) throws Exception {
        try {
            channelSftp.rename(source, target);
            return true;
        } catch (SftpException e) {
            String message = String.format("mv文件出错 :[%s] [%s] 时出错", source, target);
            throw AggregationException.asException(SftpHelperErrorCode.MV_FILE_EXCEPTION, message, e);
        }
    }

    @Override
    public InputStream getInputStream(String path, String name) throws IOException {
        String filePath = processingPath(path, name);
        try {
            return channelSftp.get(filePath);
        } catch (SftpException e) {
            String message = String.format("读取文件 : [%s] 时出错,请确认文件：[%s]存在且配置的用户有权限读取", filePath, filePath);
            log.error(message);
            throw AggregationException.asException(SftpHelperErrorCode.OPEN_FILE_ERROR, message);
        }
    }

    @Override
    public OutputStream getOutputStream(String path, String name) throws IOException {
        String filePath = processingPath(path, name);
        try {
            this.printWorkingDirectory();
            String parentDir = filePath.substring(0,
                    StringUtils.lastIndexOf(filePath, IOUtils.DIR_SEPARATOR));
            this.channelSftp.cd(parentDir);
            this.printWorkingDirectory();
            OutputStream writeOutputStream = this.channelSftp.put(filePath,
                    ChannelSftp.APPEND);
            String message = String.format(
                    "打开FTP文件[%s]获取写出流时出错,请确认文件%s有权限创建，有权限写出等", filePath,
                    filePath);
            if (null == writeOutputStream) {
                throw AggregationException.asException(
                        SftpHelperErrorCode.OPEN_FILE_ERROR, message);
            }
            return writeOutputStream;
        } catch (SftpException e) {
            String message = String.format(
                    "写出文件[%s] 时出错,请确认文件%s有权限写出, errorMessage:%s", filePath,
                    filePath, e.getMessage());
            log.error(message);
            throw AggregationException.asException(
                    SftpHelperErrorCode.OPEN_FILE_ERROR, message);
        }
    }

    private void printWorkingDirectory() {
        try {
            log.info("current working directory:{}", this.channelSftp.pwd());
        } catch (Exception e) {
            log.warn("printWorkingDirectory error:{}", e.getMessage());
        }
    }

    @Override
    public void close() throws Exception {
        if (channelSftp != null) {
            channelSftp.disconnect();

        }
        if (sftp != null) {
            sftp.disconnect();
        }
        if (session != null) {
            session.disconnect();
        }
    }

    public boolean isFileExist(String filePath) {
        boolean isExitFlag = false;
        try {
            SftpATTRS sftpATTRS = channelSftp.lstat(filePath);
            if (sftpATTRS.getSize() >= 0) {
                isExitFlag = true;
            }
        } catch (SftpException e) {
            if (e.getMessage().equalsIgnoreCase("no such file")) {
                String message = String.format("请确认您的配置项path:[%s]存在，且配置的用户有权限读取", filePath);
                log.error(message);
                throw AggregationException.asException(SftpHelperErrorCode.FILE_NOT_EXISTS, message);
            } else {
                String message = String.format("获取文件：[%s] 属性时发生I/O异常,请确认与ftp服务器的连接正常", filePath);
                log.error(message);
                throw AggregationException.asException(SftpHelperErrorCode.COMMAND_FTP_IO_EXCEPTION, message, e);
            }
        }
        return isExitFlag;
    }
}
