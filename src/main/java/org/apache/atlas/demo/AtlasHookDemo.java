package org.apache.atlas.demo;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AtlasHookDemo extends AtlasHook implements AtlasDemoConstants {

    public static final String CLUSTER_NAME = "cluster3";
    public static final String CONTENTS_CF_NAME = "default.webtable.contents@" + CLUSTER_NAME;
    public static final String WEBTABLE_NAME = "default.webtable@" + CLUSTER_NAME;

    @Override
    protected String getNumberOfRetriesPropertyKey() {
        return "atlas.hook.demo.kafka.retries";
    }

    public static void main(String[] args) {
        AtlasHookDemo atlasHookDemo = new AtlasHookDemo();
        atlasHookDemo.run();
    }

    private void run() {
        List<Referenceable> entities = new ArrayList<>();
        Referenceable namespace = createNamespace(CLUSTER_NAME);
        entities.add(namespace);
        List<Referenceable> tableEntities = createEntities("cluster3", CONTENTS_CF_NAME, WEBTABLE_NAME,
                namespace.getId()._getId());
        entities.addAll(tableEntities);

        notifyEntities("integ_user", entities);
    }

    private Referenceable createNamespace(String clusterName) {
        System.out.println("Creating namespace entity");
        Referenceable hbaseNamespace = new Referenceable(HBASE_NAMESPACE_TYPE);
        hbaseNamespace.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default@" + clusterName);
        hbaseNamespace.set(AtlasClient.NAME, "default");
        hbaseNamespace.set(AtlasClient.DESCRIPTION, "Default HBase namespace");
        hbaseNamespace.set(AtlasClient.OWNER, "hbase_admin");

        return hbaseNamespace;
    }

    private List<Referenceable> createEntities(String clusterName, String contentsCfName, String tableName, String namespaceId) {
        List<Referenceable> tableEntities = new ArrayList<>();

        Referenceable cssNsiColumn = new Referenceable(HBASE_COLUMN_TYPE);
        cssNsiColumn.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default.webtable.anchor.cssnsi@" + clusterName);
        cssNsiColumn.set(AtlasClient.NAME, "cssnsi");
        cssNsiColumn.set(AtlasClient.OWNER, "crawler");
        cssNsiColumn.set(COLUMN_ATTRIBUTE_TYPE, "string");
        tableEntities.add(cssNsiColumn);

        Referenceable myLookCaColumn = new Referenceable(HBASE_COLUMN_TYPE);
        myLookCaColumn.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default.webtable.anchor.mylookca@" + clusterName);
        myLookCaColumn.set(AtlasClient.NAME, "mylookca");
        myLookCaColumn.set(AtlasClient.OWNER, "crawler");
        myLookCaColumn.set(COLUMN_ATTRIBUTE_TYPE, "string");
        tableEntities.add(myLookCaColumn);

        Referenceable htmlColumn = new Referenceable(HBASE_COLUMN_TYPE);
        htmlColumn.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default.webtable.contents.html@" + clusterName);
        htmlColumn.set(AtlasClient.NAME, "html");
        htmlColumn.set(AtlasClient.OWNER, "crawler");
        htmlColumn.set(COLUMN_ATTRIBUTE_TYPE, "byte[]");
        tableEntities.add(htmlColumn);

        Referenceable anchorCf = new Referenceable(HBASE_COLUMN_FAMILY_TYPE);
        anchorCf.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default.webtable.anchor@" + clusterName);
        anchorCf.set(AtlasClient.NAME, "anchor");
        anchorCf.set(AtlasClient.DESCRIPTION, "The anchor column family that stores all links");
        anchorCf.set(AtlasClient.OWNER, "crawler");
        anchorCf.set(CF_ATTRIBUTE_VERSIONS, 3);
        anchorCf.set(CF_ATTRIBUTE_IN_MEMORY, true);
        anchorCf.set(CF_ATTRIBUTE_COMPRESSION, "zip");
        anchorCf.set(CF_ATTRIBUTE_BLOCK_SIZE, 128);

        anchorCf.set(CF_ATTRIBUTE_COLUMNS,
                Arrays.asList(getReferenceableId(cssNsiColumn), getReferenceableId(myLookCaColumn)));
        tableEntities.add(anchorCf);

        Referenceable contentsCf = new Referenceable(HBASE_COLUMN_FAMILY_TYPE);
        contentsCf.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, contentsCfName);
        contentsCf.set(AtlasClient.NAME, "contents");
        contentsCf.set(AtlasClient.DESCRIPTION, "The contents column family that stores the crawled content");
        contentsCf.set(AtlasClient.OWNER, "crawler");
        contentsCf.set(CF_ATTRIBUTE_VERSIONS, 1);
        contentsCf.set(CF_ATTRIBUTE_IN_MEMORY, false);
        contentsCf.set(CF_ATTRIBUTE_COMPRESSION, "lzo");
        contentsCf.set(CF_ATTRIBUTE_BLOCK_SIZE, 1024);
        contentsCf.set(CF_ATTRIBUTE_COLUMNS, Arrays.asList(getReferenceableId(htmlColumn)));
        tableEntities.add(contentsCf);

        Referenceable webTable = new Referenceable(HBASE_TABLE_TYPE);
        webTable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableName);
        webTable.set(AtlasClient.NAME, "webtable");
        webTable.set(AtlasClient.DESCRIPTION, "Table that stores crawled information");
        webTable.set(TABLE_ATTRIBUTE_IS_ENABLED, true);
        webTable.set(TABLE_ATTRIBUTE_NAMESPACE, getReferenceableId(namespaceId, HBASE_NAMESPACE_TYPE));
        webTable.set(TABLE_ATTRIBUTE_COLUMN_FAMILIES, Arrays.asList(anchorCf, contentsCf));
        tableEntities.add(webTable);

        return tableEntities;

    }

    private Id getReferenceableId(Referenceable fullReferenceable) {
        return getReferenceableId(fullReferenceable.getId()._getId(), fullReferenceable.getTypeName());
    }

    private Id getReferenceableId(String id, String typeName) {
        return new Id(id, 0, typeName);
    }

}
