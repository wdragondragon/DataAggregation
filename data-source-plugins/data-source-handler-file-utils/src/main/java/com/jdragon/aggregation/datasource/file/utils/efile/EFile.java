package com.jdragon.aggregation.datasource.file.utils.efile;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
public class EFile {

    private EFileInfo eFileInfo;

    private List<EFileDetail> eFileDetails = new ArrayList<>();

    public void addEFileDetail(EFileDetail eFileDetail) {
        eFileDetails.add(eFileDetail);
    }

    public EFileDetail getEFileDetail(int index) {
        return eFileDetails.get(index);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class EFileDetail {

        private List<List<String>> data;

        private List<String> column;

        private EFileTableInfo eFileTableInfo;

        private Map<String, String> tags = new HashMap<>();

        public void addTag(String key, String value) {
            tags.put(key, value);
        }

    }

}
