package org.apache.atlas.demo;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.demo.hbase.ColumnFamily;
import org.apache.atlas.demo.hbase.HBaseMetadata;
import org.apache.atlas.demo.hbase.Namespace;
import org.apache.atlas.demo.hbase.Table;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.persistence.Id;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.atlas.demo.AtlasDemoConstants.*;

public class HBaseMetadataImport {

    public static final String DEFAULT_OWNER = "admin";
    public static final String CLUSTER_NAME = "default";
    private final AtlasClient atlasClient;

    public HBaseMetadataImport() {
        atlasClient = new AtlasClient(new String[]{"http://localhost:21000/"}, new String[]{"admin", "admin"});
    }
    public static void main(String[] args) throws IOException, AtlasServiceException {
        HBaseMetadataImport hbaseMetadataImport = new HBaseMetadataImport();
        hbaseMetadataImport.run();
    }

    private void run() throws IOException, AtlasServiceException {
        HBaseMetadataProfiler hBaseMetadataProfiler = new HBaseMetadataProfiler();
        HBaseMetadata hBaseMetadata = hBaseMetadataProfiler.getHBaseMetadata();
        mapToAtlasEntities(hBaseMetadata);
    }

    private void mapToAtlasEntities(HBaseMetadata hBaseMetadata) throws AtlasServiceException {
        for (Namespace ns : hBaseMetadata.getNamespaces()) {
            Referenceable namespaceReferenceable = createNamespace(CLUSTER_NAME, ns);
            String hbaseNamespaceJson = InstanceSerialization.toJson(namespaceReferenceable, true);
            List<String> createdEntities = atlasClient.createEntity(hbaseNamespaceJson);
            String namespaceId = createdEntities.get(0);
            System.out.println(String.format("Created namespace: %s with ID %s", ns.getName(), namespaceId));
            for (Table t : ns.getTables()) {
                List<Referenceable> tableEntities = createTableEntities(CLUSTER_NAME, ns.getName(), t, namespaceId);
                List<String> entitiesCreated = atlasClient.createEntity(tableEntities);
                System.out.println(String.format(
                        "Create table %s with ID %s", t.getName(), entitiesCreated.get(entitiesCreated.size()-1)));
            }
        }

    }

    private Referenceable createNamespace(String clusterName, Namespace ns) {
        System.out.println("Creating namespace entity");
        Referenceable hbaseNamespace = new Referenceable(HBASE_NAMESPACE_TYPE);
        hbaseNamespace.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, ns.getName() + "@" + clusterName);
        hbaseNamespace.set(AtlasClient.NAME, ns.getName());
        hbaseNamespace.set(AtlasClient.DESCRIPTION, ns.getDescription());
        hbaseNamespace.set(AtlasClient.OWNER, DEFAULT_OWNER);
        return hbaseNamespace;
    }

    private List<Referenceable> createTableEntities(String clusterName, String nsName, Table t, String namespaceId) throws AtlasServiceException {
        List<Referenceable> cfReferenceables = new ArrayList<>();

        for (ColumnFamily cf : t.getColumnFamilies()) {
            Referenceable cfReferenceable = new Referenceable(HBASE_COLUMN_FAMILY_TYPE);
            cfReferenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, String.format(
                    "%s.%s.%s@%s", nsName, t.getName(), cf.getName(), clusterName));
            cfReferenceable.set(AtlasClient.NAME, cf.getName());
            cfReferenceable.set(AtlasClient.DESCRIPTION, cf.getDescription());
            cfReferenceable.set(AtlasClient.OWNER, DEFAULT_OWNER);
            cfReferenceable.set(CF_ATTRIBUTE_VERSIONS, cf.getVersions());
            cfReferenceable.set(CF_ATTRIBUTE_IN_MEMORY, cf.isInMemory());
            cfReferenceable.set(CF_ATTRIBUTE_COMPRESSION, cf.getCompression());
            cfReferenceable.set(CF_ATTRIBUTE_BLOCK_SIZE, cf.getBlockSize());
            cfReferenceables.add(cfReferenceable);
        }

        Referenceable tableReferenceable = new Referenceable(HBASE_TABLE_TYPE);
        tableReferenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, String.format("%s.%s:%s", nsName, t.getName(), clusterName));
        tableReferenceable.set(AtlasClient.NAME, t.getName());
        tableReferenceable.set(AtlasClient.DESCRIPTION, t.getDescription());
        tableReferenceable.set(TABLE_ATTRIBUTE_IS_ENABLED, t.isEnabled());
        tableReferenceable.set(TABLE_ATTRIBUTE_NAMESPACE, getReferenceableId(namespaceId, HBASE_NAMESPACE_TYPE));
        tableReferenceable.set(TABLE_ATTRIBUTE_COLUMN_FAMILIES, cfReferenceables);

        List<Referenceable> tableEntities = new ArrayList<>();
        tableEntities.addAll(cfReferenceables);
        tableEntities.add(tableReferenceable);
        return tableEntities;

    }

    public static Id getReferenceableId(String id, String typeName) {
        return new Id(id, 0, typeName);
    }

}
