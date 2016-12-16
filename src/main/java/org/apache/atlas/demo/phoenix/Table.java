package org.apache.atlas.demo.phoenix;

import org.apache.atlas.demo.common.Asset;

import java.util.ArrayList;
import java.util.List;

public class Table extends Asset {

    private List<Column> columns;

    public Table(String name, String description) {
        super(name, description);
        columns = new ArrayList<>();
    }


    public void addColumn(Column column) {
        columns.add(column);
    }

    public List<Column> getColumns() {
        return columns;
    }
}
