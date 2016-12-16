package org.apache.atlas.importer.phoenix.model;

import java.util.ArrayList;
import java.util.List;

public class PhoenixMetadata {

    private List<Table> tables;

    public PhoenixMetadata() {
        tables = new ArrayList<>();
    }

    public void add(Table table) {
        tables.add(table);
    }

    public List<Table> getTables() {
        return tables;
    }

}
