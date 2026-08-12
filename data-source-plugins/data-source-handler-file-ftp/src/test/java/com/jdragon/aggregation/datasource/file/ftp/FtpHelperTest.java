package com.jdragon.aggregation.datasource.file.ftp;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.junit.Test;

import java.lang.reflect.Field;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class FtpHelperTest {

    @Test
    public void selectsOnlyAnExactNameFromTheParentListing() {
        FTPFile existing = new FTPFile();
        existing.setName("existing");

        assertSame(existing, FtpHelper.findListedFile(new FTPFile[]{existing}, "existing"));
        assertNull(FtpHelper.findListedFile(new FTPFile[]{existing}, "missing"));
    }

    @Test
    public void resolvesAllClientPathsBelowTheLoginDirectory() throws Exception {
        FtpHelper helper = new FtpHelper();
        setBaseDirectory(helper, "/home/ftp-user");

        assertEquals("/home/ftp-user", helper.resolveRemotePath("/"));
        assertEquals("/home/ftp-user/inbox/file.txt", helper.resolveRemotePath("/inbox/file.txt"));
        assertEquals("/home/ftp-user/home/ftp-user/legacy.txt",
                helper.resolveRemotePath("/home/ftp-user/legacy.txt"));
        assertEquals("/home/ftp-user/legacy.txt",
                helper.resolveLegacyRemotePath("/home/ftp-user/legacy.txt"));
        assertEquals("/inbox/file.txt", helper.toLogicalPath("/home/ftp-user/inbox/file.txt"));
        assertThrows(IllegalArgumentException.class,
                () -> helper.resolveRemotePath("/inbox/../../outside.txt"));
        assertThrows(IllegalArgumentException.class,
                () -> helper.toLogicalPath("/home/another-user/file.txt"));
    }

    @Test
    public void logicalDirectoryNamedLikeTheLoginRootDoesNotResolveBackToRoot() throws Exception {
        FtpHelper helper = new FtpHelper();
        setBaseDirectory(helper, "/data");

        assertEquals("/data/data", helper.resolveRemotePath("/data"));
        assertEquals("/data", helper.toLogicalPath("/data/data"));
    }

    @Test
    public void consumesCompletionReplyAfterTransferAbortedReply() throws Exception {
        FtpHelper helper = new FtpHelper();
        StubFtpClient client = new StubFtpClient(FTPReply.TRANSFER_ABORTED, false, false, true);
        setFtpClient(helper, client);
        setPendingCommand(helper, true);

        helper.completePartialTransfer("/large-file.bin");

        assertEquals(2, client.completeCalls);
        assertEquals(0L, client.restartOffset);
        assertFalse(pendingCommand(helper));
    }

    @Test
    public void acceptsTransferAbortedReplyWithoutTrailingCompletion() throws Exception {
        FtpHelper helper = new FtpHelper();
        StubFtpClient client = new StubFtpClient(FTPReply.TRANSFER_ABORTED, true, false);
        setFtpClient(helper, client);
        setPendingCommand(helper, true);

        helper.completePartialTransfer("/large-file.bin");

        assertEquals(2, client.completeCalls);
        assertEquals(0, client.soTimeout);
        assertEquals(0L, client.restartOffset);
        assertFalse(pendingCommand(helper));
    }

    @Test(expected = java.io.IOException.class)
    public void rejectsUnexpectedPartialTransferReply() throws Exception {
        FtpHelper helper = new FtpHelper();
        StubFtpClient client = new StubFtpClient(FTPReply.COMMAND_NOT_IMPLEMENTED, false, false);
        setFtpClient(helper, client);
        setPendingCommand(helper, true);

        try {
            helper.completePartialTransfer("/large-file.bin");
        } finally {
            assertEquals(1, client.completeCalls);
            assertEquals(0L, client.restartOffset);
            assertFalse(pendingCommand(helper));
        }
    }

    private static void setFtpClient(FtpHelper helper, FTPClient client) throws Exception {
        Field field = FtpHelper.class.getDeclaredField("ftpClient");
        field.setAccessible(true);
        field.set(helper, client);
    }

    private static void setPendingCommand(FtpHelper helper, boolean pending) throws Exception {
        Field field = FtpHelper.class.getDeclaredField("pendingCommand");
        field.setAccessible(true);
        field.setBoolean(helper, pending);
    }

    private static void setBaseDirectory(FtpHelper helper, String baseDirectory) throws Exception {
        Field field = FtpHelper.class.getDeclaredField("baseDirectory");
        field.setAccessible(true);
        field.set(helper, baseDirectory);
    }

    private static boolean pendingCommand(FtpHelper helper) throws Exception {
        Field field = FtpHelper.class.getDeclaredField("pendingCommand");
        field.setAccessible(true);
        return field.getBoolean(helper);
    }

    private static final class StubFtpClient extends FTPClient {
        private final int replyCode;
        private final boolean timeoutWhenResultsExhausted;
        private final boolean[] completeResults;
        private int completeCalls;
        private int soTimeout;
        private long restartOffset = -1L;

        private StubFtpClient(int replyCode, boolean timeoutWhenResultsExhausted,
                              boolean... completeResults) {
            this.replyCode = replyCode;
            this.timeoutWhenResultsExhausted = timeoutWhenResultsExhausted;
            this.completeResults = completeResults;
        }

        @Override
        public int getReplyCode() {
            return replyCode;
        }

        @Override
        public boolean completePendingCommand() throws java.io.IOException {
            if (completeCalls >= completeResults.length && timeoutWhenResultsExhausted) {
                completeCalls++;
                throw new SocketTimeoutException("no trailing reply");
            }
            int index = Math.min(completeCalls, completeResults.length - 1);
            completeCalls++;
            return completeResults[index];
        }

        @Override
        public int getSoTimeout() {
            return soTimeout;
        }

        @Override
        public void setSoTimeout(int timeout) throws SocketException {
            soTimeout = timeout;
        }

        @Override
        public void setRestartOffset(long offset) {
            restartOffset = offset;
        }
    }
}
