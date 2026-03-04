package com.jdragon.aggregation.datasource.file.utils.efile;

import lombok.Data;

import java.util.Map;

@Data
public class EFileInfo {

    private String Entity;

    private String type;

    private String dataTime;

    private String date;

    private String areaCode;

    private String fileType;

    private String objectName;

    private String clz;

    private String commitId;

    private Map<String,String> tags;

}
