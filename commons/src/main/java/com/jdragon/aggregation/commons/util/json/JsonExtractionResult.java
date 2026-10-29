package com.jdragon.aggregation.commons.util.json;

import com.alibaba.fastjson.JSONArray;

public final class JsonExtractionResult {
    private final JSONArray rows;
    private final int sourceRowCount;

    public JsonExtractionResult(JSONArray rows, int sourceRowCount) {
        this.rows = rows;
        this.sourceRowCount = sourceRowCount;
    }

    public JSONArray getRows() {
        return rows;
    }

    public int getSourceRowCount() {
        return sourceRowCount;
    }

    public JSONArray rows() {
        return rows;
    }

    public int sourceRowCount() {
        return sourceRowCount;
    }
}
