package com.jdragon.aggregation.core.streaming;

import com.jdragon.aggregation.core.streaming.PartitionedSpillStore.PartitionRow;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

public class PartitionReader implements AutoCloseable {

    private final BufferedReader reader;

    public PartitionReader(Path path) throws IOException {
        this.reader = Files.newBufferedReader(path, StandardCharsets.UTF_8);
    }

    public void readAll(Consumer<PartitionRow> consumer) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isEmpty()) {
                continue;
            }
            consumer.accept(RowCodec.decode(line, PartitionRow.class));
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
