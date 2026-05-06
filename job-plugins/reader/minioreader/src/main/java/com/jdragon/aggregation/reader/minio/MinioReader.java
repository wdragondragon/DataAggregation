package com.jdragon.aggregation.reader.minio;

import com.jdragon.aggregation.datasource.file.s3.minio.MinioSourcePlugin;
import com.jdragon.aggregation.filetablereader.CommonFileTableReader;

public class MinioReader extends CommonFileTableReader {

    public MinioReader() {
        super(new MinioSourcePlugin(), true);
    }
}
