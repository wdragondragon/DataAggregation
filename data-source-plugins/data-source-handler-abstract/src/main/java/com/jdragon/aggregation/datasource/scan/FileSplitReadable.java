package com.jdragon.aggregation.datasource.scan;

import com.jdragon.aggregation.commons.util.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public interface FileSplitReadable {
    List<String> listSplits(String path, String pattern) throws IOException;

    void readSplit(String splitPath, String fileType, Consumer<Map<String, Object>> rowConsumer, Configuration options) throws IOException;
}
