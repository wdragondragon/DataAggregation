package com.jdragon.aggregation.reader.sftp;

import com.jdragon.aggregation.datasource.file.sftp.SftpHelper;
import com.jdragon.aggregation.filetablereader.CommonFileTableReader;

public class SftpReader extends CommonFileTableReader {

    public SftpReader() {
        super(new SftpHelper());
    }
}
