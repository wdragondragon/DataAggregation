package com.jdragon.aggregation.datasource;

import lombok.Data;

import java.util.List;

@Data
public class InsertDataDTO {
    private BaseDataSourceDTO baseDataSourceDTO;

    private String tableName;

    private List<String> field;

    private List<List<String>> data;

    private Integer batchSize;

    private boolean truncate;

    public InsertDataDTO(List<String> field, List<List<String>> data) {
        this.field = field;
        this.data = data;
    }

}
