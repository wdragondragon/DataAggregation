package com.jdragon.aggregation.datasource.file.utils.efile;

import lombok.Data;

import java.util.List;
import java.util.Map;

@Data
public class EFileTag {

    private EFileTableInfo eFileTableInfo;

    private List<List<String>> datas;

    private Map<String,String> columns;

}
