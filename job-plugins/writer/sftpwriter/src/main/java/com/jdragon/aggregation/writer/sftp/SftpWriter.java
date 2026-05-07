package com.jdragon.aggregation.writer.sftp;

import com.jdragon.aggregation.datasource.file.sftp.SftpHelper;
import com.jdragon.aggregation.filetablewriter.CommonFileTableWriter;

public class SftpWriter extends CommonFileTableWriter {

    public SftpWriter() {
        super(new SftpHelper());
    }
}
