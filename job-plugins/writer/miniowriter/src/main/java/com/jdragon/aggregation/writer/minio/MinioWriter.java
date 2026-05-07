package com.jdragon.aggregation.writer.minio;

import com.jdragon.aggregation.datasource.file.s3.minio.MinioSourcePlugin;
import com.jdragon.aggregation.filetablewriter.CommonFileTableWriter;

public class MinioWriter extends CommonFileTableWriter {

    public MinioWriter() {
        super(new MinioSourcePlugin());
    }
}
