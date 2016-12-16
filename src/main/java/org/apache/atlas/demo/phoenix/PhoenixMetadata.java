package org.apache.atlas.demo.phoenix;

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
