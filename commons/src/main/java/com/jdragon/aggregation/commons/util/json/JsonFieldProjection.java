package com.jdragon.aggregation.commons.util.json;

/** A configured output field and the response object path from which it is read. */
public final class JsonFieldProjection {
    private final String name;
    private final String sourceName;
    private final String parentNode;

    public JsonFieldProjection(String name, String sourceName, String parentNode) {
        this.name = name;
        this.sourceName = sourceName;
        this.parentNode = parentNode;
    }

    public String getName() {
        return name;
    }

    public String getSourceName() {
        return sourceName;
    }

    public String getParentNode() {
        return parentNode;
    }
}
