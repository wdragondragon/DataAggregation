package com.jdragon.aggregation.datasource.file.utils.efile;

import lombok.Data;

import java.math.BigInteger;

@Data
public class EFileError {


    private BigInteger id;

    private String ErrorType;

    private String ErrorFile;

    private Integer ErrorCode;

    private String ErrorDescribe;

    private String ErrorMsg;

    private String CreateType;

    private String UpdateTime;

    private Integer Status;

}
