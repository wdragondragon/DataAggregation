package com.jdragon.aggregation.reader.ftp;

import com.jdragon.aggregation.datasource.file.ftp.FtpHelper;
import com.jdragon.aggregation.filetablereader.CommonFileTableReader;

public class FtpReader extends CommonFileTableReader {

    public FtpReader() {
        super(new FtpHelper());
    }
}
