package com.jdragon.aggregation.transfer;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static org.junit.Assert.assertFalse;

public class ContractDependencyBoundaryTest {
    @Test
    public void contractMainCodeStaysFrameworkAndStorageIndependent() throws IOException {
        Path current = Paths.get(System.getProperty("user.dir")).toAbsolutePath();
        Path module = "file-transfer-contract".equals(current.getFileName().toString())
                ? current : current.resolve("file-transfer/file-transfer-contract");
        Path mainSources = module.resolve(Paths.get("src", "main", "java"));
        try (Stream<Path> sourceFiles = Files.walk(mainSources)) {
            String source = sourceFiles.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".java"))
                    .map(this::read)
                    .reduce("", String::concat);
            assertFalse(source.contains("org.springframework"));
            assertFalse(source.contains("com.alibaba.fastjson"));
            assertFalse(source.contains("pluginloader"));
            assertFalse(source.contains("TransferFileSystem"));
            assertFalse(source.contains("data.source"));
        }
    }

    private String read(Path path) {
        try {
            return Files.readString(path);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to inspect " + path, exception);
        }
    }
}
