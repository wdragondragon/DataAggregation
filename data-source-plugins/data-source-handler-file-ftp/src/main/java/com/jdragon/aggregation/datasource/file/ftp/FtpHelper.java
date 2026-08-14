package com.jdragon.aggregation.datasource.file.ftp;

import com.jdragon.aggregation.commons.exception.AggregationException;
import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.transfer.StorageCapabilities;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.datasource.file.transfer.TransferFilePage;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileSystem;
import com.jdragon.aggregation.datasource.file.transfer.TransferWriteSession;
import com.jdragon.aggregation.datasource.file.utils.FileParser;
import com.jdragon.aggregation.pluginloader.spi.AbstractPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.net.ftp.*;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;


@Slf4j
public class FtpHelper extends AbstractPlugin implements TransferFileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(FtpHelper.class);
    private static final int COPY_BUFFER_SIZE = 64 * 1024;
    private static final int OPTIONAL_COMPLETION_REPLY_TIMEOUT_MILLIS = 100;
    private FTPClient ftpClient = null;

    private String ftpTLS;

    private String host;

    private String username;

    private String password;

    private int port;

    private String connectMode;

    private int timeout;

    private String baseDirectory;

    private boolean pathCaseSensitive = false;

    private boolean pendingCommand;

    @Override
    public boolean connect(Configuration configuration) {
        ftpTLS = configuration.getString("ftpTLS", Constant.DEFAULT_FTP_TLS);
        host = configuration.getNecessaryValue("host", FtpHelperErrorCode.REQUIRED_VALUE);
        username = configuration.getNecessaryValue("username", FtpHelperErrorCode.REQUIRED_VALUE);
        password = configuration.getNecessaryValue("password", FtpHelperErrorCode.REQUIRED_VALUE);
        port = configuration.getInt("port", Constant.DEFAULT_FTP_PORT);
        connectMode = configuration.getString("connectMode", Constant.DEFAULT_FTP_CONNECT_PATTERN);
        timeout = configuration.getInt("timeout", Constant.DEFAULT_TIMEOUT);
        pathCaseSensitive = configuration.getBool("pathCaseSensitive", false);

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
            baseDirectory = normalizeRemotePath(ftpClient.printWorkingDirectory());
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
        return true;
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
        String filePath = resolveLegacyRemotePath(processingPath(path, name));
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
        FTPFile[] ftpFiles = this.ftpClient.listFiles(resolveLegacyRemotePath(dir));
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
        FTPFile ftpFile = this.ftpClient.mlistFile(resolveLegacyRemotePath(processingPath(dir, fileName)));
        if (ftpFile == null) {
            return false;
        }
        return ftpFile.isFile();
    }

    @Override
    public void mkdir(String directoryPath) throws IOException {
        String targetDirectory = resolveLegacyRemotePath(directoryPath);
        if (StringUtils.isBlank(targetDirectory)) {
            return;
        }
        String originalDirectory = normalizeRemotePath(ftpClient.printWorkingDirectory());
        // ftp server不支持递归创建目录,只能一级一级创建
        boolean absolutePath = StringUtils.startsWith(targetDirectory, "/");
        StringBuilder dirPath = new StringBuilder(absolutePath ? "/" : "");
        String[] dirSplit = StringUtils.split(targetDirectory, "/");
        try {
            for (String dirName : dirSplit) {
                if (dirPath.length() > 0 && !StringUtils.endsWith(dirPath.toString(), "/")) {
                    dirPath.append("/");
                }
                dirPath.append(dirName);
                // 如果directoryPath目录不存在,则创建
                if (ftpClient.changeWorkingDirectory(dirPath.toString())) {
                    continue;
                }
                int replayCode = this.ftpClient.mkd(dirPath.toString());
                if (replayCode != FTPReply.COMMAND_OK
                        && replayCode != FTPReply.PATHNAME_CREATED) {
                    String message = String.format("创建FTP目录[%s]失败, 状态码:%s, 回复:%s",
                            dirPath, this.ftpClient.getReplyCode(), this.ftpClient.getReplyString());
                    log.error(message);
                    throw AggregationException.asException(FtpHelperErrorCode.COMMAND_FTP_IO_EXCEPTION, message);
                }
            }
        } finally {
            if (StringUtils.isNotBlank(originalDirectory)) {
                ftpClient.changeWorkingDirectory(originalDirectory);
            }
        }
    }

    @Override
    public void rm(String path) throws IOException {
        try {
            this.ftpClient.deleteFile(resolveLegacyRemotePath(path));
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
            return this.ftpClient.rename(resolveLegacyRemotePath(from), resolveLegacyRemotePath(to));
        } catch (IOException e) {
            throw AggregationException.asException(FtpHelperErrorCode.MV_FILE_EXCEPTION, e);
        }
    }

    @Override
    public InputStream getInputStream(String path, String name) throws IOException {
        String filePath = resolveLegacyRemotePath(processingPath(path, name));
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
        String directoryPath = resolveLegacyRemotePath(path);
        String filePath = resolveLegacyRemotePath(processingPath(path, name));
        try {
            this.printWorkingDirectory();
            this.ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
            this.ftpClient.enterLocalPassiveMode();

            boolean b = this.ftpClient.changeWorkingDirectory(directoryPath);
            this.printWorkingDirectory();
            if (!b) {
                String message = String.format("切换FTP目录[%s]失败, 状态码:%s, 回复:%s",
                        directoryPath, this.ftpClient.getReplyCode(), this.ftpClient.getReplyString());
                log.error(message);
                throw AggregationException.asException(FtpHelperErrorCode.OPEN_FILE_ERROR, message);
            }
            OutputStream writeOutputStream = this.ftpClient
                    .storeFileStream(name);
            LOG.info("ftp获取文件流{},状态码：{},回复：{}", filePath, this.ftpClient.getReplyCode(), this.ftpClient.getReplyString());
            String message = String.format(
                    "打开FTP文件[%s]获取写出流时出错,请确认文件%s有权限创建，有权限写出等", filePath,
                    filePath);
            if (null == writeOutputStream) {
                throw AggregationException.asException(
                        FtpHelperErrorCode.OPEN_FILE_ERROR, message);
            }

            pendingCommand = true;
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

    String resolveRemotePath(String path) {
        String normalized = normalizeLogicalPath(path);
        String base = normalizeRemotePath(StringUtils.defaultIfBlank(baseDirectory, "/"));
        if ("/".equals(normalized)) {
            return base;
        }
        return processingPath(base, StringUtils.removeStart(normalized, "/"));
    }

    String resolveLegacyRemotePath(String path) {
        String normalized = normalizeRemotePath(path);
        validateRemotePath(normalized);
        if (StringUtils.isBlank(normalized) || normalized.startsWith("/")) {
            return normalized;
        }
        String base = normalizeRemotePath(StringUtils.defaultIfBlank(baseDirectory, ""));
        return StringUtils.isBlank(base) ? normalized : processingPath(base, normalized);
    }

    String toLogicalPath(String remotePath) {
        String normalized = normalizeRemotePath(remotePath);
        String base = normalizeRemotePath(StringUtils.defaultIfBlank(baseDirectory, "/"));
        if (!isWithinBase(normalized, base)) {
            throw new IllegalArgumentException("FTP path is outside the login directory: " + remotePath);
        }
        if (normalized.equals(base)) {
            return "/";
        }
        if ("/".equals(base)) {
            return normalized.startsWith("/") ? normalized : "/" + normalized;
        }
        return normalized.substring(base.length());
    }

    private String normalizeLogicalPath(String path) {
        String normalized = normalizeRemotePath(path);
        if (StringUtils.isBlank(normalized) || ".".equals(normalized)) {
            return "/";
        }
        validateRemotePath(normalized);
        return normalized.startsWith("/") ? normalized : "/" + normalized;
    }

    private void validateRemotePath(String path) {
        if (path.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("FTP path must not contain NUL");
        }
        for (String segment : path.split("/")) {
            if ("..".equals(segment)) {
                throw new IllegalArgumentException("FTP path must not contain '..'");
            }
        }
    }

    private boolean isWithinBase(String path, String base) {
        return "/".equals(base) || path.equals(base) || path.startsWith(base + "/");
    }

    private String normalizeRemotePath(String path) {
        if (path == null) {
            return "";
        }
        String normalized = path.trim().replace('\\', '/');
        while (normalized.contains("//")) {
            normalized = normalized.replace("//", "/");
        }
        if (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
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
        if (!pendingCommand) {
            return;
        }
        try {
            boolean completed = this.ftpClient.completePendingCommand();
            if (!completed) {
                String message = String.format("发送completePendingCommand指令失败, 状态码:%s, 回复:%s",
                        this.ftpClient.getReplyCode(), this.ftpClient.getReplyString());
                throw AggregationException.asException(
                        FtpHelperErrorCode.COMPLETE_PENDING_COMMAND_ERROR, message);
            }
        } catch (IOException e) {
            throw AggregationException.asException(
                    FtpHelperErrorCode.COMPLETE_PENDING_COMMAND_ERROR, e);
        } finally {
            pendingCommand = false;
        }
    }

    @Override
    public void readFile(String absPath, String fileType, java.util.function.Consumer<java.util.Map<String, Object>> row, Configuration options) throws IOException {
        // 从绝对路径中提取目录和文件名
        int lastSlash = absPath.lastIndexOf('/');
        String path, name;
        if (lastSlash >= 0) {
            path = absPath.substring(0, lastSlash);
            name = absPath.substring(lastSlash + 1);
        } else {
            path = "/";
            name = absPath;
        }
        
        InputStream inputStream = getInputStream(path, name);
        if (inputStream == null) {
            throw new IOException("Cannot get input stream for file: " + absPath);
        }
        try (InputStream is = inputStream) {
            FileParser.FileFormat format = FileParser.FileFormat.fromString(fileType);
            FileParser.parseInputStream(is, format, "UTF-8", row, options);
        } finally {
            completePendingReadCommand(absPath);
        }
    }

    private void completePendingReadCommand(String absPath) throws IOException {
        if (ftpClient == null || !ftpClient.isConnected()) {
            return;
        }
        boolean completed = ftpClient.completePendingCommand();
        if (!completed) {
            String message = String.format("完成FTP文件[%s]读取命令失败, 状态码:%s, 回复:%s",
                    absPath, ftpClient.getReplyCode(), ftpClient.getReplyString());
            LOG.error(message);
            throw new IOException(message);
        }
    }

    @Override
    public TransferFileEntry stat(String path) throws IOException {
        String logical = normalizeLogicalPath(path);
        if ("/".equals(logical)) {
            return new TransferFileEntry("/", "/", true, 0L, 0L, null);
        }
        String resolved = resolveRemotePath(path);
        FTPFile file = findFile(resolved);
        if (file == null) {
            throw new NoSuchFileException(path);
        }
        long modified = file.getTimestamp() == null ? 0L : file.getTimestamp().getTimeInMillis();
        return new TransferFileEntry(toLogicalPath(resolved), remoteName(resolved), file.isDirectory(),
                file.isDirectory() ? 0L : file.getSize(), modified, null);
    }

    @Override
    public TransferFilePage listPage(String directory, String cursor, int pageSize) throws IOException {
        String resolved = resolveRemotePath(directory);
        FTPFile[] files = ftpClient.listFiles(resolved);
        List<TransferFileEntry> entries = new ArrayList<>();
        for (FTPFile file : files) {
            if (StringUtils.equalsAny(file.getName(), ".", "..")) {
                continue;
            }
            String path = toLogicalPath(processingPath(resolved, file.getName()));
            long modified = file.getTimestamp() == null ? 0L : file.getTimestamp().getTimeInMillis();
            entries.add(new TransferFileEntry(path, file.getName(), file.isDirectory(),
                    file.isDirectory() ? 0L : file.getSize(), modified, null));
        }
        entries.sort(Comparator.comparing(TransferFileEntry::directory).reversed()
                .thenComparing(TransferFileEntry::name, String.CASE_INSENSITIVE_ORDER));
        int offset = parseCursor(cursor);
        int limit = Math.max(1, Math.min(pageSize, 1_000));
        int from = Math.min(offset, entries.size());
        int to = Math.min(from + limit, entries.size());
        boolean truncated = to < entries.size();
        return new TransferFilePage(entries.subList(from, to), truncated ? String.valueOf(to) : null, truncated);
    }

    @Override
    public InputStream openRead(String path, long offset, long length) throws IOException {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("offset and length must be non-negative");
        }
        String resolved = resolveRemotePath(path);
        FTPFile file = findFile(resolved);
        if (file == null || !file.isFile()) {
            throw new NoSuchFileException(path);
        }
        if (offset > file.getSize()) {
            throw new IOException("read offset exceeds file size: " + offset);
        }
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        ftpClient.setRestartOffset(offset);
        InputStream stream = ftpClient.retrieveFileStream(resolved);
        if (stream == null) {
            ftpClient.setRestartOffset(0);
            throw new IOException("FTP server did not open read stream for " + resolved
                    + ", reply: " + ftpClient.getReplyString());
        }
        pendingCommand = true;
        boolean reachesEnd = length >= file.getSize() - offset;
        return new FtpRangeInputStream(stream, length, resolved, reachesEnd);
    }

    @Override
    public TransferWriteSession prepareWrite(String temporaryPath, String sessionId) throws IOException {
        String logical = normalizeLogicalPath(temporaryPath);
        String resolved = resolveRemotePath(logical);
        createParentDirectory(resolved);
        FTPFile file = findFile(resolved);
        if (file != null && file.isDirectory()) {
            throw new IOException("temporary path is a directory: " + temporaryPath);
        }
        long offset = file == null ? 0L : file.getSize();
        if (file == null) {
            try (InputStream empty = InputStream.nullInputStream()) {
                if (!ftpClient.storeFile(resolved, empty)) {
                    throw new IOException("failed to create FTP temporary file " + resolved
                            + ", reply: " + ftpClient.getReplyString());
                }
            }
        }
        return new TransferWriteSession(sessionId, logical, offset, Map.of());
    }

    @Override
    public Optional<TransferWriteSession> recoverWriteSession(
            TransferWriteSession session, long confirmedOffset) throws IOException {
        if (confirmedOffset < 0) {
            throw new IllegalArgumentException("confirmedOffset must be non-negative");
        }
        String path = resolveRemotePath(session.temporaryPath());
        FTPFile existing = findFile(path);
        if (existing == null || existing.isDirectory() || existing.getSize() < confirmedOffset) {
            return Optional.empty();
        }
        // FTP REST resumes at the requested offset. An unconfirmed tail is
        // overwritten by subsequent source blocks without a full-file copy.
        return Optional.of(new TransferWriteSession(session.sessionId(),
                session.temporaryPath(), confirmedOffset, session.metadata()));
    }

    @Override
    public long append(TransferWriteSession session, long offset, InputStream source, long length)
            throws IOException {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("offset and length must be non-negative");
        }
        String path = resolveRemotePath(session.temporaryPath());
        FTPFile existing = findFile(path);
        long size = existing == null ? 0L : existing.getSize();
        if (size < offset) {
            throw new IOException("temporary file is shorter than the write offset, expected at least "
                    + offset + " but was " + size);
        }
        ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
        ftpClient.setRestartOffset(offset);
        OutputStream output = ftpClient.storeFileStream(path);
        if (output == null) {
            ftpClient.setRestartOffset(0);
            throw new IOException("FTP server did not open write stream for " + path
                    + ", reply: " + ftpClient.getReplyString());
        }
        pendingCommand = true;
        long written = 0L;
        IOException failure = null;
        try (OutputStream target = output) {
            byte[] buffer = new byte[COPY_BUFFER_SIZE];
            long remaining = length;
            while (remaining > 0) {
                int read = source.read(buffer, 0, (int) Math.min(buffer.length, remaining));
                if (read < 0) {
                    break;
                }
                if (read == 0) {
                    continue;
                }
                target.write(buffer, 0, read);
                written += read;
                remaining -= read;
            }
        } catch (IOException e) {
            failure = e;
        }
        try {
            completePendingTransfer(path);
        } catch (IOException completionError) {
            if (failure != null) {
                failure.addSuppressed(completionError);
            } else {
                failure = completionError;
            }
        }
        if (failure != null) {
            throw failure;
        }
        return written;
    }

    @Override
    public void commit(TransferWriteSession session, String targetPath, boolean replaceExisting)
            throws IOException {
        move(session.temporaryPath(), targetPath, replaceExisting);
    }

    @Override
    public void abort(TransferWriteSession session) throws IOException {
        ftpClient.deleteFile(resolveRemotePath(session.temporaryPath()));
    }

    @Override
    public void move(String sourcePath, String targetPath, boolean replaceExisting) throws IOException {
        String source = resolveRemotePath(sourcePath);
        String target = resolveRemotePath(targetPath);
        if (findFile(target) != null) {
            if (!replaceExisting) {
                throw new FileAlreadyExistsException(targetPath);
            }
            if (!ftpClient.deleteFile(target)) {
                throw new IOException("failed to replace FTP target " + target
                        + ", reply: " + ftpClient.getReplyString());
            }
        }
        createParentDirectory(target);
        if (!ftpClient.rename(source, target)) {
            throw new IOException("failed to rename FTP path " + source + " to " + target
                    + ", reply: " + ftpClient.getReplyString());
        }
    }

    @Override
    public void delete(String path) throws IOException {
        deleteResolved(resolveRemotePath(path));
    }

    private void deleteResolved(String resolved) throws IOException {
        FTPFile file = findFile(resolved);
        if (file == null) {
            return;
        }
        if (file.isDirectory()) {
            FTPFile[] children = ftpClient.listFiles(resolved);
            for (FTPFile child : children) {
                if (!StringUtils.equalsAny(child.getName(), ".", "..")) {
                    deleteResolved(processingPath(resolved, child.getName()));
                }
            }
            if (!ftpClient.removeDirectory(resolved)) {
                throw new IOException("failed to remove FTP directory " + resolved);
            }
        } else if (!ftpClient.deleteFile(resolved)) {
            throw new IOException("failed to delete FTP file " + resolved);
        }
    }

    @Override
    public StorageCapabilities capabilities() {
        return new StorageCapabilities(true, true, true, false, false, false,
                Set.of("SHA-256"), pathCaseSensitive);
    }

    @Override
    public boolean transferExists(String path) throws IOException {
        return findFile(resolveRemotePath(path)) != null;
    }

    private FTPFile findFile(String path) throws IOException {
        String resolved = normalizeRemotePath(path);
        if (StringUtils.isBlank(resolved) || "/".equals(resolved)) {
            FTPFile root = new FTPFile();
            root.setName("/");
            root.setType(FTPFile.DIRECTORY_TYPE);
            return root;
        }
        int slash = resolved.lastIndexOf('/');
        String parent = slash < 0 ? "." : slash == 0 ? "/" : resolved.substring(0, slash);
        return findListedFile(ftpClient.listFiles(parent), remoteName(resolved));
    }

    static FTPFile findListedFile(FTPFile[] listed, String expectedName) {
        if (listed == null || expectedName == null) {
            return null;
        }
        for (FTPFile candidate : listed) {
            if (candidate != null && expectedName.equals(candidate.getName())) {
                return candidate;
            }
        }
        return null;
    }

    private void createParentDirectory(String path) throws IOException {
        int slash = path.lastIndexOf('/');
        if (slash > 0) {
            mkdir(path.substring(0, slash));
        }
    }

    private String remoteName(String path) {
        int slash = path.lastIndexOf('/');
        return slash < 0 ? path : path.substring(slash + 1);
    }

    private int parseCursor(String cursor) {
        if (StringUtils.isBlank(cursor)) {
            return 0;
        }
        try {
            return Math.max(0, Integer.parseInt(cursor));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid FTP page cursor: " + cursor, e);
        }
    }

    private void completePendingTransfer(String path) throws IOException {
        try {
            if (!ftpClient.completePendingCommand()) {
                throw new IOException("FTP data transfer did not complete for " + path
                        + ", reply: " + ftpClient.getReplyString());
            }
        } finally {
            ftpClient.setRestartOffset(0);
            pendingCommand = false;
        }
    }

    void completePartialTransfer(String path) throws IOException {
        try {
            boolean completed = ftpClient.completePendingCommand();
            int replyCode = ftpClient.getReplyCode();
            if (!completed && replyCode == FTPReply.TRANSFER_ABORTED) {
                consumeOptionalCompletionReply(path);
                completed = true;
            }
            if (!completed) {
                throw new IOException("FTP partial data transfer did not complete for " + path
                        + ", reply: " + ftpClient.getReplyString());
            }
        } finally {
            ftpClient.setRestartOffset(0);
            pendingCommand = false;
        }
    }

    private void consumeOptionalCompletionReply(String path) throws IOException {
        int originalTimeout = ftpClient.getSoTimeout();
        try {
            ftpClient.setSoTimeout(OPTIONAL_COMPLETION_REPLY_TIMEOUT_MILLIS);
            try {
                if (!ftpClient.completePendingCommand()) {
                    throw new IOException("FTP partial data transfer returned an unexpected trailing reply for "
                            + path + ", reply: " + ftpClient.getReplyString());
                }
            } catch (SocketTimeoutException ignored) {
                // Some servers terminate an intentionally shortened RETR with only a 426 reply.
            }
        } finally {
            ftpClient.setSoTimeout(originalTimeout);
        }
    }

    private final class FtpRangeInputStream extends java.io.FilterInputStream {
        private long remaining;
        private final String path;
        private final boolean reachesEnd;
        private boolean closed;

        private FtpRangeInputStream(InputStream input, long remaining, String path, boolean reachesEnd) {
            super(input);
            this.remaining = remaining;
            this.path = path;
            this.reachesEnd = reachesEnd;
        }

        @Override
        public int read() throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int value = super.read();
            if (value >= 0) {
                remaining--;
            }
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            if (remaining <= 0) {
                return -1;
            }
            int read = super.read(buffer, offset, (int) Math.min(length, remaining));
            if (read > 0) {
                remaining -= read;
            }
            return read;
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            IOException failure = null;
            try {
                super.close();
            } catch (IOException e) {
                failure = e;
            }
            try {
                if (reachesEnd) {
                    completePendingTransfer(path);
                } else {
                    completePartialTransfer(path);
                }
            } catch (IOException completionError) {
                if (failure != null) {
                    failure.addSuppressed(completionError);
                } else {
                    failure = completionError;
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    @Override
    public void close() throws Exception {
        pendingCommand = false;
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
