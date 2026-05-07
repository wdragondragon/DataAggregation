package com.jdragon.aggregation.writer.ftp;

import com.jdragon.aggregation.datasource.file.ftp.FtpHelper;
import com.jdragon.aggregation.filetablewriter.CommonFileTableWriter;

public class FtpWriter extends CommonFileTableWriter {

    public FtpWriter() {
        super(new FtpHelper());
    }
}
