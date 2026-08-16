package com.jdragon.aggregation.datasource.file.local;

import com.jdragon.aggregation.commons.util.Configuration;
import com.jdragon.aggregation.datasource.file.transfer.AsyncUploadOutputStream;
import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.datasource.file.transfer.TransferFilePage;
import com.jdragon.aggregation.datasource.file.transfer.TransferWriteSession;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class LocalSourcePluginTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void rejectsPathsOutsideConfiguredRoot() throws Exception {
        Path root = temporaryFolder.newFolder("root").toPath();
        Path outside = temporaryFolder.newFile("outside.txt").toPath();
        LocalSourcePlugin plugin = connected(root);

        assertThrows(AccessDeniedException.class, () -> plugin.stat(outside.toString()));
        assertThrows(AccessDeniedException.class, () -> plugin.openRead("../outside.txt", 0, 1));
    }

    @Test
    public void listsDirectoriesFirstWithStableOffsetPagination() throws Exception {
        Path root = temporaryFolder.newFolder("paged").toPath();
        Files.createDirectories(root.resolve("z-directory"));
        Files.writeString(root.resolve("a.txt"), "a");
        Files.writeString(root.resolve("b.txt"), "b");
        LocalSourcePlugin plugin = connected(root);

        TransferFilePage first = plugin.listPage(".", null, 2);
        assertTrue(first.truncated());
        assertEquals("2", first.nextCursor());
        assertEquals(List.of("z-directory", "a.txt"), names(first));

        TransferFilePage second = plugin.listPage(".", first.nextCursor(), 2);
        assertFalse(second.truncated());
        assertNull(second.nextCursor());
        assertEquals(List.of("b.txt"), names(second));
    }

    @Test
    public void readsOnlyRequestedRange() throws Exception {
        Path root = temporaryFolder.newFolder("range").toPath();
        Files.writeString(root.resolve("source.bin"), "0123456789", StandardCharsets.UTF_8);
        LocalSourcePlugin plugin = connected(root);

        try (InputStream input = plugin.openRead("source.bin", 3, 4)) {
            assertArrayEquals("3456".getBytes(StandardCharsets.UTF_8), input.readAllBytes());
        }
    }

    @Test
    public void resumesAppendOnlyAtConfirmedOffset() throws Exception {
        Path root = temporaryFolder.newFolder("resume").toPath();
        LocalSourcePlugin plugin = connected(root);

        TransferWriteSession session = plugin.prepareWrite(".transfer/item.part", "item-1");
        assertEquals(0L, session.confirmedOffset());
        assertEquals(3L, plugin.append(session, 0,
                new ByteArrayInputStream("abc".getBytes(StandardCharsets.UTF_8)), 3));

        TransferWriteSession resumed = plugin.prepareWrite(".transfer/item.part", "item-1");
        assertEquals(3L, resumed.confirmedOffset());
        assertThrows(java.io.IOException.class, () -> plugin.append(resumed, 2,
                new ByteArrayInputStream(new byte[]{1}), 1));
        assertEquals(3L, plugin.append(resumed, 3,
                new ByteArrayInputStream("def".getBytes(StandardCharsets.UTF_8)), 3));
        assertEquals("abcdef", Files.readString(root.resolve(".transfer/item.part")));
    }

    @Test
    public void commitsTemporaryFileAndHonorsReplacePolicy() throws Exception {
        Path root = temporaryFolder.newFolder("commit").toPath();
        LocalSourcePlugin plugin = connected(root);
        Files.writeString(root.resolve("target.txt"), "old");

        TransferWriteSession denied = write(plugin, ".transfer/denied.part", "new");
        assertThrows(java.nio.file.FileAlreadyExistsException.class,
                () -> plugin.commit(denied, "target.txt", false));

        TransferWriteSession replacement = write(plugin, ".transfer/replacement.part", "new");
        plugin.commit(replacement, "target.txt", true);
        assertEquals("new", Files.readString(root.resolve("target.txt")));
        assertFalse(Files.exists(root.resolve(".transfer/replacement.part")));
    }

    @Test
    public void recursivelyDeletesDirectoryTree() throws Exception {
        Path root = temporaryFolder.newFolder("delete").toPath();
        Path directory = Files.createDirectories(root.resolve("a/b/c"));
        Files.writeString(directory.resolve("payload.txt"), "payload");
        LocalSourcePlugin plugin = connected(root);

        plugin.delete("a");

        assertFalse(Files.exists(root.resolve("a")));
    }

    @Test
    public void asyncUploadStreamUsesBoundedPipeAndPropagatesAllBytes() throws Exception {
        byte[] block = "0123456789abcdef".repeat(4_096).getBytes(StandardCharsets.UTF_8);
        AtomicLong received = new AtomicLong();
        AtomicReference<String> digest = new AtomicReference<>();
        try (AsyncUploadOutputStream output = new AsyncUploadOutputStream(8 * 1024, input -> {
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[4 * 1024];
            int read;
            while ((read = input.read(buffer)) >= 0) {
                if (read > 0) {
                    received.addAndGet(read);
                    sha256.update(buffer, 0, read);
                }
            }
            digest.set(HexFormat.of().formatHex(sha256.digest()));
        })) {
            for (int index = 0; index < 32; index++) {
                output.write(block);
            }
        }

        MessageDigest expected = MessageDigest.getInstance("SHA-256");
        for (int index = 0; index < 32; index++) {
            expected.update(block);
        }
        assertEquals((long) block.length * 32, received.get());
        assertEquals(HexFormat.of().formatHex(expected.digest()), digest.get());
    }

    private LocalSourcePlugin connected(Path root) {
        Configuration configuration = Configuration.newDefault();
        configuration.set("rootPath", root.toString());
        LocalSourcePlugin plugin = new LocalSourcePlugin();
        assertTrue(plugin.connect(configuration));
        return plugin;
    }

    private TransferWriteSession write(LocalSourcePlugin plugin, String path, String value) throws Exception {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        TransferWriteSession session = plugin.prepareWrite(path, path);
        assertEquals(bytes.length, plugin.append(session, session.confirmedOffset(),
                new ByteArrayInputStream(bytes), bytes.length));
        return session;
    }

    private List<String> names(TransferFilePage page) {
        return page.entries().stream().map(TransferFileEntry::name).toList();
    }
}
