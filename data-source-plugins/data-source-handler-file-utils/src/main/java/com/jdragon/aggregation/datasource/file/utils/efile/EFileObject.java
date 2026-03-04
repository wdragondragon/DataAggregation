package com.jdragon.aggregation.datasource.file.utils.efile;

import lombok.Data;

import java.util.List;

@Data
public class EFileObject {
    private EFileInfo eFileInfo;

    private List<EFileTag> datas;

}
