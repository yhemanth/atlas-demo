package org.apache.atlas.demo.hbase;

import org.apache.atlas.demo.common.Asset;

import java.util.ArrayList;
import java.util.List;

public class Namespace extends Asset {

    private List<Table> tables;

    public Namespace(String name, String description) {
        super(name, description);
        tables = new ArrayList<>();
    }

    public void addTable(Table table) {
        tables.add(table);
    }

    public List<Table> getTables() {
        return tables;
    }
}
