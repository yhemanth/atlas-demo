package org.apache.atlas.importer.phoenix.model;

import org.apache.atlas.importer.common.Asset;

public class Column extends Asset {

    private final String columnType;
    private final String columnFamily;

    public Column(String name, String description, String columnType, String columnFamily) {
        super(name, description);
        this.columnType = columnType;
        this.columnFamily = (columnFamily==null) ? "Null" : columnFamily;
    }

    public String getColumnType() {
        return columnType;
    }

    public String getColumnFamily() {
        return columnFamily;
    }
}
