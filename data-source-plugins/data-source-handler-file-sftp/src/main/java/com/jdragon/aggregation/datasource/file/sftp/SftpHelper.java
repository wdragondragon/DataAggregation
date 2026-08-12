package com.jdragon.aggregation.datasource.file.sftp;

import com.jcraft.jsch.*;
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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.*;

@Slf4j
public class SftpHelper extends AbstractPlugin implements TransferFileSystem {

    private static final int COPY_BUFFER_SIZE = 64 * 1024;
    private static final StorageCapabilities CAPABILITIES =
            new StorageCapabilities(true, true, true, false, false, false, Set.of("SHA-256"), true);
    private static final String LEGACY_KEX_ALGORITHMS =
            "diffie-hellman-group14-sha1,diffie-hellman-group-exchange-sha1,diffie-hellman-group1-sha1";
    private static final String LEGACY_HOST_KEY_ALGORITHMS = "ssh-rsa,ssh-dss";
    private static final String LEGACY_CIPHER_ALGORITHMS = "aes128-cbc,aes192-cbc,aes256-cbc,3des-cbc";
    private static final String LEGACY_MAC_ALGORITHMS = "hmac-md5,hmac-md5-96";

    private Session session = null;

    private ChannelSftp channelSftp = null;

    private Channel sftp = null;

    private String host;

    private String username;

    private String password;

    private int port;

    private int timeout;

    private String baseDirectory;

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
            session.setConfig(buildSessionConfig(configuration));
            session.setTimeout(timeout); // 设置timeout时间
            session.connect(); // 通过Session建立链接
            channelSftp = (ChannelSftp) session.openChannel("sftp"); // 打开SFTP通道
            channelSftp.connect(); // 建立SFTP通道的连接
            baseDirectory = normalizeRemotePath(channelSftp.pwd());
            sftp = session.openChannel("exec");
            //设置命令传输编码
            //String fileEncoding = System.getProperty("file.encoding");
            //channelSftp.setFilenameEncoding(fileEncoding);
        } catch (JSchException | SftpException e) {
            if (isAlgorithmNegotiationFailure(e)) {
                String message = "与SFTP服务器协商SSH算法失败。请优先由服务端启用现代SSH算法；"
                        + "仅当确认服务器只能使用旧算法时，才在数据源中开启 allowLegacyAlgorithms 后重试。"
                        + "原始错误: " + StringUtils.defaultString(e.getMessage());
                log.error(message, e);
                throw AggregationException.asException(SftpHelperErrorCode.FAIL_LOGIN, message, e);
            }
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
        return true;
    }

    static Properties buildSessionConfig(Configuration configuration) {
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        if (!Boolean.TRUE.equals(configuration.getBool("allowLegacyAlgorithms", false))) {
            return config;
        }
        config.put("kex", appendAlgorithms(JSch.getConfig("kex"), LEGACY_KEX_ALGORITHMS));
        config.put("server_host_key", appendAlgorithms(JSch.getConfig("server_host_key"), LEGACY_HOST_KEY_ALGORITHMS));
        config.put("cipher.c2s", appendAlgorithms(JSch.getConfig("cipher.c2s"), LEGACY_CIPHER_ALGORITHMS));
        config.put("cipher.s2c", appendAlgorithms(JSch.getConfig("cipher.s2c"), LEGACY_CIPHER_ALGORITHMS));
        config.put("mac.c2s", appendAlgorithms(JSch.getConfig("mac.c2s"), LEGACY_MAC_ALGORITHMS));
        config.put("mac.s2c", appendAlgorithms(JSch.getConfig("mac.s2c"), LEGACY_MAC_ALGORITHMS));
        config.put("PubkeyAcceptedAlgorithms",
                appendAlgorithms(JSch.getConfig("PubkeyAcceptedAlgorithms"), LEGACY_HOST_KEY_ALGORITHMS));
        return config;
    }

    static boolean isAlgorithmNegotiationFailure(Exception exception) {
        String message = exception == null ? null : exception.getMessage();
        return StringUtils.containsIgnoreCase(message, "algorithm negotiation fail")
                || StringUtils.containsIgnoreCase(message, "no matching key exchange method")
                || StringUtils.containsIgnoreCase(message, "no matching host key type")
                || StringUtils.containsIgnoreCase(message, "no matching cipher")
                || StringUtils.containsIgnoreCase(message, "no matching mac");
    }

    private static String appendAlgorithms(String defaults, String additions) {
        LinkedHashSet<String> algorithms = new LinkedHashSet<>();
        addAlgorithms(algorithms, defaults);
        addAlgorithms(algorithms, additions);
        return String.join(",", algorithms);
    }

    private static void addAlgorithms(Set<String> algorithms, String values) {
        if (StringUtils.isBlank(values)) {
            return;
        }
        for (String algorithm : values.split(",")) {
            String normalized = StringUtils.trimToNull(algorithm);
            if (normalized != null) {
                algorithms.add(normalized);
            }
        }
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
        // Preserve the original FileHelper contract: absolute paths are
        // passed to the SFTP server unchanged. Relative paths retain the
        // caller's original behavior and are not silently rebased.
        mkdirResolved(normalizeRemotePath(directoryPath));
    }

    /** Creates an already resolved SFTP path one level at a time. */
    private void mkdirResolved(String directoryPath) throws IOException {
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
            this.channelSftp.cd(path);
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

        try (InputStream is = getInputStream(path, name)) {
            if (is == null) {
                throw new IOException("Cannot get input stream for file: " + absPath);
            }

            FileParser.FileFormat format = FileParser.FileFormat.fromString(fileType);
            FileParser.parseInputStream(is, format, "UTF-8", row, options);
        }
    }

    @Override
    public TransferFileEntry stat(String path) throws IOException {
        String resolved = resolveRemotePath(path);
        try {
            SftpATTRS attributes = channelSftp.lstat(resolved);
            String logical = toLogicalPath(resolved);
            return new TransferFileEntry(logical, "/".equals(logical) ? "/" : remoteName(resolved), attributes.isDir(),
                    attributes.isDir() ? 0L : attributes.getSize(), attributes.getMTime() * 1_000L, null);
        } catch (SftpException e) {
            if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                throw new NoSuchFileException(path);
            }
            throw new IOException("failed to stat SFTP path " + path, e);
        }
    }

    @Override
    public TransferFilePage listPage(String directory, String cursor, int pageSize) throws IOException {
        String resolved = resolveRemotePath(directory);
        try {
            @SuppressWarnings("unchecked")
            Vector<ChannelSftp.LsEntry> listed = channelSftp.ls(resolved);
            List<TransferFileEntry> entries = new ArrayList<>();
            for (ChannelSftp.LsEntry child : listed) {
                if (StringUtils.equalsAny(child.getFilename(), ".", "..")) {
                    continue;
                }
                SftpATTRS attributes = child.getAttrs();
                entries.add(new TransferFileEntry(toLogicalPath(joinRemote(resolved, child.getFilename())),
                        child.getFilename(), attributes.isDir(),
                        attributes.isDir() ? 0L : attributes.getSize(),
                        attributes.getMTime() * 1_000L, null));
            }
            entries.sort(Comparator.comparing(TransferFileEntry::directory).reversed()
                    .thenComparing(TransferFileEntry::name, String.CASE_INSENSITIVE_ORDER));
            int offset = parseCursor(cursor);
            int limit = Math.max(1, Math.min(pageSize, 1_000));
            int from = Math.min(offset, entries.size());
            int to = Math.min(from + limit, entries.size());
            boolean truncated = to < entries.size();
            return new TransferFilePage(entries.subList(from, to),
                    truncated ? String.valueOf(to) : null, truncated);
        } catch (SftpException e) {
            throw new IOException("failed to list SFTP directory " + directory, e);
        }
    }

    @Override
    public InputStream openRead(String path, long offset, long length) throws IOException {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("offset and length must be non-negative");
        }
        String resolved = resolveRemotePath(path);
        TransferFileEntry entry = stat(path);
        if (entry.directory()) {
            throw new IOException("cannot read SFTP directory " + path);
        }
        if (offset > entry.size()) {
            throw new IOException("read offset exceeds file size: " + offset);
        }
        try {
            return new LimitedInputStream(channelSftp.get(resolved, null, offset), length);
        } catch (SftpException e) {
            throw new IOException("failed to open SFTP range for " + path, e);
        }
    }

    @Override
    public TransferWriteSession prepareWrite(String temporaryPath, String sessionId) throws IOException {
        String resolved = resolveRemotePath(temporaryPath);
        createParentDirectory(resolved);
        long offset = 0L;
        try {
            SftpATTRS attributes = channelSftp.lstat(resolved);
            if (attributes.isDir()) {
                throw new IOException("temporary path is a directory: " + temporaryPath);
            }
            offset = attributes.getSize();
        } catch (SftpException e) {
            if (e.id != ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                throw new IOException("failed to inspect SFTP temporary path " + temporaryPath, e);
            }
            try (OutputStream ignored = channelSftp.put(resolved, ChannelSftp.OVERWRITE)) {
            } catch (SftpException createError) {
                throw new IOException("failed to create SFTP temporary path " + temporaryPath, createError);
            }
        }
        return new TransferWriteSession(sessionId, resolved, offset, Map.of());
    }

    @Override
    public long append(TransferWriteSession session, long offset, InputStream source, long length)
            throws IOException {
        if (offset < 0 || length < 0) {
            throw new IllegalArgumentException("offset and length must be non-negative");
        }
        String path = resolveRemotePath(session.temporaryPath());
        final SftpATTRS current;
        try {
            current = channelSftp.lstat(path);
        } catch (SftpException e) {
            throw new IOException("failed to inspect SFTP temporary path " + session.temporaryPath(), e);
        }
        if (current.getSize() != offset) {
            throw new IOException("temporary file offset mismatch, expected " + offset
                    + " but was " + current.getSize());
        }
        long written = 0L;
        try (OutputStream target = channelSftp.put(path, ChannelSftp.APPEND)) {
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
        } catch (SftpException e) {
            throw new IOException("failed to append SFTP temporary path " + path, e);
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
        delete(session.temporaryPath());
    }

    @Override
    public void move(String sourcePath, String targetPath, boolean replaceExisting) throws IOException {
        String source = resolveRemotePath(sourcePath);
        String target = resolveRemotePath(targetPath);
        if (resolvedExists(target)) {
            if (!replaceExisting) {
                throw new FileAlreadyExistsException(targetPath);
            }
            deleteResolved(target);
        }
        createParentDirectory(target);
        try {
            channelSftp.rename(source, target);
        } catch (SftpException e) {
            throw new IOException("failed to rename SFTP path " + source + " to " + target, e);
        }
    }

    @Override
    public void delete(String path) throws IOException {
        deleteResolved(resolveRemotePath(path));
    }

    private void deleteResolved(String resolved) throws IOException {
        final SftpATTRS attributes;
        try {
            attributes = channelSftp.lstat(resolved);
        } catch (SftpException e) {
            if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                return;
            }
            throw new IOException("failed to inspect SFTP path before delete " + resolved, e);
        }
        try {
            if (attributes.isDir()) {
                @SuppressWarnings("unchecked")
                Vector<ChannelSftp.LsEntry> children = channelSftp.ls(resolved);
                for (ChannelSftp.LsEntry child : children) {
                    if (!StringUtils.equalsAny(child.getFilename(), ".", "..")) {
                        deleteResolved(joinRemote(resolved, child.getFilename()));
                    }
                }
                channelSftp.rmdir(resolved);
            } else {
                channelSftp.rm(resolved);
            }
        } catch (SftpException e) {
            throw new IOException("failed to delete SFTP path " + resolved, e);
        }
    }

    @Override
    public StorageCapabilities capabilities() {
        return CAPABILITIES;
    }

    @Override
    public String initialPath() {
        return normalizeRemotePath(StringUtils.defaultIfBlank(baseDirectory, "/"));
    }

    @Override
    public boolean transferExists(String path) throws IOException {
        return resolvedExists(resolveRemotePath(path));
    }

    private boolean resolvedExists(String resolved) throws IOException {
        try {
            channelSftp.lstat(resolved);
            return true;
        } catch (SftpException e) {
            if (e.id == ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                return false;
            }
            throw new IOException("failed to inspect SFTP path " + resolved, e);
        }
    }

    private void createParentDirectory(String path) throws IOException {
        int slash = path.lastIndexOf('/');
        if (slash > 0) {
            mkdirResolved(path.substring(0, slash));
        }
    }

    String resolveRemotePath(String path) {
        String normalized = normalizeRemotePath(path);
        if (normalized.isBlank()) {
            return normalizeRemotePath(StringUtils.defaultIfBlank(baseDirectory, "/"));
        }
        if (normalized.startsWith("/")) {
            return normalized;
        }
        return joinRemote(baseDirectory, normalized);
    }

    String toLogicalPath(String remotePath) {
        return normalizeRemotePath(remotePath);
    }

    private String normalizeRemotePath(String path) {
        String normalized = StringUtils.trimToEmpty(path).replace('\\', '/');
        while (normalized.contains("//")) {
            normalized = normalized.replace("//", "/");
        }
        while (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private String joinRemote(String parent, String child) {
        return normalizeRemotePath(parent) + "/" + normalizeRemotePath(child);
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
            throw new IllegalArgumentException("invalid SFTP page cursor: " + cursor, e);
        }
    }

    private static final class LimitedInputStream extends java.io.FilterInputStream {
        private long remaining;

        private LimitedInputStream(InputStream input, long remaining) {
            super(input);
            this.remaining = remaining;
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
