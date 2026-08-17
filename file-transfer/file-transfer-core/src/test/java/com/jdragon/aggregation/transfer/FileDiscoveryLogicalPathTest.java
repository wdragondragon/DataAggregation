package com.jdragon.aggregation.transfer;

import com.jdragon.aggregation.datasource.file.transfer.TransferFileEntry;
import com.jdragon.aggregation.datasource.file.transfer.TransferFilePage;
import com.jdragon.aggregation.datasource.file.transfer.StorageCapabilities;
import com.jdragon.aggregation.transfer.model.TransferSelection;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class FileDiscoveryLogicalPathTest {

    @Test
    public void recursiveDiscoveryUsesEachLogicalDirectoryOnce() throws Exception {
        List<String> listedDirectories = new ArrayList<>();
        TestTransferFileSystem source = new TestTransferFileSystem() {
            @Override
            public TransferFileEntry stat(String path) {
                return new TransferFileEntry(path, "/".equals(path) ? "/" : "data",
                        true, 0L, 0L, null);
            }

            @Override
            public TransferFilePage listPage(String directory, String cursor, int pageSize) {
                listedDirectories.add(directory);
                if ("/".equals(directory)) {
                    return new TransferFilePage(List.of(
                            new TransferFileEntry("/data", "data", true, 0L, 0L, null)),
                            null, false);
                }
                return new TransferFilePage(List.of(
                        new TransferFileEntry("/data/child.txt", "child.txt", false, 1L, 0L, null)),
                        null, false);
            }
        };
        TransferSelection selection = new TransferSelection("/", List.of(), true,
                List.of(), null, List.of(), null, null, null, null, 10);
        List<DiscoveredFile> files = new ArrayList<>();

        new FileDiscovery().discover(source, selection, files::add);

        assertEquals(List.of("/", "/data"), listedDirectories);
        assertEquals(List.of("data/child.txt"), files.stream().map(DiscoveredFile::relativePath).toList());
    }

    @Test
    public void topLevelObjectFileUsesItsNameAsRelativePath() throws Exception {
        TestTransferFileSystem source = new TestTransferFileSystem() {
            @Override
            public TransferFileEntry stat(String path) {
                return new TransferFileEntry("gx-data-1.0.0.jar", "gx-data-1.0.0.jar",
                        false, 1L, 0L, null);
            }
        };
        TransferSelection selection = new TransferSelection("/gx-data-1.0.0.jar", List.of(), false,
                List.of(), null, List.of(), null, null, null, null, 10);
        List<DiscoveredFile> files = new ArrayList<>();

        new FileDiscovery().discover(source, selection, files::add);

        assertEquals(List.of("gx-data-1.0.0.jar"),
                files.stream().map(DiscoveredFile::relativePath).toList());
    }

    @Test
    public void objectStorageRootAcceptsRelativeObjectKeys() throws Exception {
        TestTransferFileSystem source = new TestTransferFileSystem() {
            @Override
            public TransferFileEntry stat(String path) {
                return new TransferFileEntry("", "", true, 0L, 0L, null);
            }

            @Override
            public TransferFilePage listPage(String directory, String cursor, int pageSize) {
                assertEquals("", directory);
                return new TransferFilePage(List.of(
                        new TransferFileEntry("gx-data-1.0.0.jar", "gx-data-1.0.0.jar",
                                false, 1L, 0L, null)), null, false);
            }
        };
        TransferSelection selection = new TransferSelection("/", List.of(), true,
                List.of(), null, List.of(), null, null, null, null, 10);
        List<DiscoveredFile> files = new ArrayList<>();

        new FileDiscovery().discover(source, selection, files::add);

        assertEquals(List.of("gx-data-1.0.0.jar"),
                files.stream().map(DiscoveredFile::relativePath).toList());
    }

    @Test
    public void caseInsensitiveSourceAcceptsPathsWhoseCaseDiffersFromConfiguredRoot() throws Exception {
        TestTransferFileSystem source = new TestTransferFileSystem() {
            @Override
            public StorageCapabilities capabilities() {
                return new StorageCapabilities(false, false, false, false, false, false,
                        Set.of(), false);
            }

            @Override
            public TransferFileEntry stat(String path) {
                return new TransferFileEntry("/actual", "actual", true, 0L, 0L, null);
            }

            @Override
            public TransferFilePage listPage(String directory, String cursor, int pageSize) {
                return new TransferFilePage(List.of(
                        new TransferFileEntry("/ACTUAL/file.txt", "file.txt", false, 1L, 0L, null)),
                        null, false);
            }
        };
        TransferSelection selection = new TransferSelection("/Actual", List.of(), true,
                List.of(), null, List.of(), null, null, null, null, 10);
        List<DiscoveredFile> files = new ArrayList<>();

        new FileDiscovery().discover(source, selection, files::add);

        assertEquals(List.of("file.txt"), files.stream().map(DiscoveredFile::relativePath).toList());
    }
}
