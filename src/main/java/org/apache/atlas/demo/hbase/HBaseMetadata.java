package org.apache.atlas.demo.hbase;

import java.util.ArrayList;
import java.util.List;

public class HBaseMetadata {

    private List<Namespace> namespaces;

    public HBaseMetadata() {
        namespaces = new ArrayList<>();
    }

    public void add(Namespace namespace) {
        namespaces.add(namespace);
    }

    public List<Namespace> getNamespaces() {
        return namespaces;
    }
}
