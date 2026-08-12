package com.jdragon.aggregation.datasource.file.sftp;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SftpHelperTest {

    @Test
    public void preservesAbsolutePathsAndUsesPwdForRelativePaths() throws Exception {
        SftpHelper helper = new SftpHelper();
        setBaseDirectory(helper, "/home/sftp-user");

        assertEquals("/", helper.resolveRemotePath("/"));
        assertEquals("/inbox/file.txt", helper.resolveRemotePath("/inbox/file.txt"));
        assertEquals("/home/sftp-user/legacy.txt", helper.resolveRemotePath("/home/sftp-user/legacy.txt"));
        assertEquals("/home/sftp-user/inbox/file.txt", helper.resolveRemotePath("inbox/file.txt"));
        assertEquals("/home/sftp-user/inbox/file.txt", helper.toLogicalPath("/home/sftp-user/inbox/file.txt"));
        assertEquals("/home/sftp-user", helper.initialPath());
    }

    @Test
    public void absoluteDirectoryNamedLikeTheLoginRootIsNotRebased() throws Exception {
        SftpHelper helper = new SftpHelper();
        setBaseDirectory(helper, "/data");

        assertEquals("/data", helper.resolveRemotePath("/data"));
        assertEquals("/data", helper.toLogicalPath("/data"));
    }

    @Test
    public void mkdirPreservesAbsolutePathFromTheBrowser() throws Exception {
        SftpHelper helper = new SftpHelper();
        setBaseDirectory(helper, "/home/sftp-user");
        RecordingChannelSftp channel = new RecordingChannelSftp();
        setChannel(helper, channel);

        helper.mkdir("/upload/codex/new-folder");

        assertTrue(channel.createdPaths.contains("/upload"));
        assertTrue(channel.createdPaths.contains("/upload/codex"));
        assertTrue(channel.createdPaths.contains("/upload/codex/new-folder"));
        assertFalse(channel.createdPaths.contains("/home/sftp-user/upload"));
    }

    @Test
    public void keepsModernJschDefaultsUnlessLegacyAlgorithmsAreExplicitlyEnabled() {
        Properties defaults = SftpHelper.buildSessionConfig(Configuration.newDefault());

        assertEquals("no", defaults.getProperty("StrictHostKeyChecking"));
        assertFalse(defaults.containsKey("kex"));
        assertFalse(defaults.containsKey("server_host_key"));

        Configuration legacyConfiguration = Configuration.newDefault();
        legacyConfiguration.set("allowLegacyAlgorithms", true);
        Properties legacy = SftpHelper.buildSessionConfig(legacyConfiguration);

        assertTrue(legacy.getProperty("kex").contains("diffie-hellman-group1-sha1"));
        assertTrue(legacy.getProperty("server_host_key").contains("ssh-rsa"));
        assertTrue(legacy.getProperty("server_host_key").contains("ssh-dss"));
        assertTrue(legacy.getProperty("cipher.c2s").contains("aes128-cbc"));
        assertTrue(legacy.getProperty("mac.s2c").contains("hmac-md5"));
    }

    @Test
    public void identifiesNegotiationFailuresForActionableLoginErrors() {
        assertTrue(SftpHelper.isAlgorithmNegotiationFailure(
                new Exception("Algorithm negotiation fail")));
        assertTrue(SftpHelper.isAlgorithmNegotiationFailure(
                new Exception("no matching host key type found")));
        assertFalse(SftpHelper.isAlgorithmNegotiationFailure(new Exception("Auth fail")));
    }

    private static void setBaseDirectory(SftpHelper helper, String baseDirectory) throws Exception {
        Field field = SftpHelper.class.getDeclaredField("baseDirectory");
        field.setAccessible(true);
        field.set(helper, baseDirectory);
    }

    private static void setChannel(SftpHelper helper, RecordingChannelSftp channel) throws Exception {
        Field field = SftpHelper.class.getDeclaredField("channelSftp");
        field.setAccessible(true);
        field.set(helper, channel);
    }

    private static final class RecordingChannelSftp extends ChannelSftp {
        private final List<String> createdPaths = new ArrayList<>();

        @Override
        public String pwd() {
            return "/home/sftp-user";
        }

        @Override
        public SftpATTRS lstat(String path) throws SftpException {
            throw new SftpException(SSH_FX_NO_SUCH_FILE, "no such file");
        }

        @Override
        public void mkdir(String path) {
            createdPaths.add(path);
        }
    }
}
