package org.apache.atlas.demo.common;

public class Asset {

    private final String name;
    private final String description;

    public Asset(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }
}
