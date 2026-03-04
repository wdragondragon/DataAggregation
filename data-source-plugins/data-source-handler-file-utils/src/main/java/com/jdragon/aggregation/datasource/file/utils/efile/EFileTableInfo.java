package com.jdragon.aggregation.datasource.file.utils.efile;

import lombok.Data;

@Data
public class EFileTableInfo {

    private String clz;

    private String tableName;

    private String tableCode;

    private String planDate;

    private Integer num;

    private String commitId;
}
