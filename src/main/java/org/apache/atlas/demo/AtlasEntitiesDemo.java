package org.apache.atlas.demo;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;

import java.util.Arrays;
import java.util.List;

public class AtlasEntitiesDemo implements AtlasDemoConstants {

    public static final String LOCAL_CLUSTER = "cluster1";
    public static final String REMOTE_CLUSTER = "cluster2";
    public static final String LOCAL_WEBTABLE_NAME = "default.webtable@" + LOCAL_CLUSTER;
    public static final String REMOTE_WEBTABLE_NAME = "default.webtable@" + REMOTE_CLUSTER;
    private final AtlasClient atlasClient;

    public AtlasEntitiesDemo(String atlasServiceUrl) {
        atlasClient = new AtlasClient(new String[]{atlasServiceUrl}, new String[]{"admin", "admin"});
    }

    public static void main(String[] args) throws AtlasServiceException, JSONException {
        AtlasEntitiesDemo atlasEntitiesDemo = new AtlasEntitiesDemo(args[0]);
        atlasEntitiesDemo.run();
    }

    private void run() throws AtlasServiceException, JSONException {
        // create an entity
        String localNamespaceId = createNamespace(LOCAL_CLUSTER);

        // create multiple entities - table, column families, columns
        String localTableId = createTable(localNamespaceId, LOCAL_CLUSTER, LOCAL_WEBTABLE_NAME);

        // retrieve entities (by GUID and unique attributes)
        retrieveEntity(localNamespaceId);
        retrieveEntity(localTableId);
        retrieveEntityByUniqueAttribute(HBASE_TABLE_TYPE, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                LOCAL_WEBTABLE_NAME);

        // update an entity - modify attribute value of some attribute
        updateEntity(localTableId);
        retrieveEntity(localTableId);

        // add lineage related data
        String remoteNamespaceId = createNamespace(REMOTE_CLUSTER);
        String remoteTableId= createTable(remoteNamespaceId, REMOTE_CLUSTER, REMOTE_WEBTABLE_NAME);
        String replicationProcessEntityId = createReplicationProcessEntity(localTableId, remoteTableId);
        retrieveEntity(replicationProcessEntityId);

        // delete an entity
        deleteEntity(localTableId);
        retrieveEntity(localTableId);
    }

    private String createReplicationProcessEntity(String localTableId, String remoteTableId)
            throws AtlasServiceException {
        System.out.println("Creating a replication instance for lineage.");
        Referenceable referenceable = new Referenceable(HBASE_REPLICATION_PROCESS_TYPE);
        String processName = "Replication: " + LOCAL_WEBTABLE_NAME + "->" + REMOTE_WEBTABLE_NAME;
        referenceable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, processName);
        referenceable.set(AtlasClient.NAME, processName);
        referenceable.set(AtlasClient.PROCESS_ATTRIBUTE_INPUTS,
                Arrays.asList(getReferenceableId(localTableId, HBASE_TABLE_TYPE)));
        referenceable.set(AtlasClient.PROCESS_ATTRIBUTE_OUTPUTS,
                Arrays.asList(getReferenceableId(remoteTableId, HBASE_TABLE_TYPE)));
        referenceable.set(REPLICATION_ENABLED, true);
        referenceable.set(REPLICATION_SCHEDULE, "daily");

        System.out.println(InstanceSerialization.toJson(referenceable, true));

        List<String> entity = atlasClient.createEntity(Arrays.asList(referenceable));
        Utils.printDelimiter();
        return entity.get(0);
    }

    private void deleteEntity(String id) throws AtlasServiceException {
        System.out.println("Deleting entity with GUID: " + id);
        AtlasClient.EntityResult entityResult = atlasClient.deleteEntities(id);
        for (String entity : entityResult.getDeletedEntities()) {
            System.out.println("Entity deleted: " + entity);
        }
        Utils.printDelimiter();
    }

    private void updateEntity(String tableId) throws AtlasServiceException {
        System.out.println("Updating table state to disabled");
        Referenceable tableEntity = new Referenceable(HBASE_TABLE_TYPE);
        tableEntity.set(TABLE_ATTRIBUTE_IS_ENABLED, false);
        String entityJson = InstanceSerialization.toJson(tableEntity, true);
        System.out.println(entityJson);
        AtlasClient.EntityResult entityResult = atlasClient.updateEntity(tableId, tableEntity);
        List<String> updateEntities = entityResult.getUpdateEntities();
        for (String entity : updateEntities) {
            System.out.println("Updated Entity ID: " + entity);
        }
        Utils.printDelimiter();
    }

    private void retrieveEntityByUniqueAttribute(
            String typeName, String uniqueAttributeName, String uniqueAttributeValue) throws AtlasServiceException {
        System.out.println("Retrieving entity with type: "
                + typeName + "/" + uniqueAttributeName+"=" + uniqueAttributeValue);
        Referenceable entity = atlasClient.getEntity(typeName, uniqueAttributeName, uniqueAttributeValue);
        String entityJson = InstanceSerialization.toJson(entity, true);
        System.out.println(entityJson);
        Utils.printDelimiter();
    }

    private void retrieveEntity(String guid) throws AtlasServiceException {
        System.out.println("Retrieving entity with GUID: " + guid);
        Referenceable entity = atlasClient.getEntity(guid);
        String entityJson = InstanceSerialization.toJson(entity, true);
        System.out.println(entityJson);
        Utils.printDelimiter();
    }

    private String createTable(String namespaceId, String clusterName, String tableName)
            throws AtlasServiceException, JSONException {
        System.out.println("Creating Table, Column Family & Column entities");
        Referenceable cssNsiColumn = new Referenceable(HBASE_COLUMN_TYPE);
        cssNsiColumn.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default.webtable.anchor.cssnsi@" + clusterName);
        cssNsiColumn.set(AtlasClient.NAME, "cssnsi");
        cssNsiColumn.set(AtlasClient.OWNER, "crawler");
        cssNsiColumn.set(COLUMN_ATTRIBUTE_TYPE, "string");

        Referenceable myLookCaColumn = new Referenceable(HBASE_COLUMN_TYPE);
        myLookCaColumn.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default.webtable.anchor.mylookca@" + clusterName);
        myLookCaColumn.set(AtlasClient.NAME, "mylookca");
        myLookCaColumn.set(AtlasClient.OWNER, "crawler");
        myLookCaColumn.set(COLUMN_ATTRIBUTE_TYPE, "string");

        Referenceable htmlColumn = new Referenceable(HBASE_COLUMN_TYPE);
        htmlColumn.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default.webtable.contents.html@" + clusterName);
        htmlColumn.set(AtlasClient.NAME, "html");
        htmlColumn.set(AtlasClient.OWNER, "crawler");
        htmlColumn.set(COLUMN_ATTRIBUTE_TYPE, "byte[]");

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

        Referenceable contentsCf = new Referenceable(HBASE_COLUMN_FAMILY_TYPE);
        contentsCf.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default.webtable.contents@" + clusterName);
        contentsCf.set(AtlasClient.NAME, "contents");
        contentsCf.set(AtlasClient.DESCRIPTION, "The contents column family that stores the crawled content");
        contentsCf.set(AtlasClient.OWNER, "crawler");
        contentsCf.set(CF_ATTRIBUTE_VERSIONS, 1);
        contentsCf.set(CF_ATTRIBUTE_IN_MEMORY, false);
        contentsCf.set(CF_ATTRIBUTE_COMPRESSION, "lzo");
        contentsCf.set(CF_ATTRIBUTE_BLOCK_SIZE, 1024);
        contentsCf.set(CF_ATTRIBUTE_COLUMNS, Arrays.asList(getReferenceableId(htmlColumn)));

        Referenceable webTable = new Referenceable(HBASE_TABLE_TYPE);
        webTable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, tableName);
        webTable.set(AtlasClient.NAME, "webtable");
        webTable.set(AtlasClient.DESCRIPTION, "Table that stores crawled information");
        webTable.set(TABLE_ATTRIBUTE_IS_ENABLED, true);
        webTable.set(TABLE_ATTRIBUTE_NAMESPACE, getReferenceableId(namespaceId, HBASE_NAMESPACE_TYPE));
        webTable.set(TABLE_ATTRIBUTE_COLUMN_FAMILIES, Arrays.asList(anchorCf, contentsCf));

        List<Referenceable> entities =
                Arrays.asList(cssNsiColumn, myLookCaColumn, htmlColumn, anchorCf, contentsCf, webTable);

        JSONArray entitiesJson = new JSONArray(entities.size());

        System.out.print("[");
        for (Referenceable entity : entities) {
            String entityJson = InstanceSerialization.toJson(entity, true);
            System.out.print(entityJson);
            System.out.println(",");
            entitiesJson.put(entityJson);
        }
        System.out.println("]");

        List<String> entitiesCreated = atlasClient.createEntity(entities);
        for (String entity : entitiesCreated) {
            System.out.println("Entity created: " + entity);
        }
        Utils.printDelimiter();
        return entitiesCreated.get(entitiesCreated.size()-1);
    }

    private Id getReferenceableId(Referenceable fullReferenceable) {
        return getReferenceableId(fullReferenceable.getId()._getId(), fullReferenceable.getTypeName());
    }

    private Id getReferenceableId(String id, String typeName) {
        return new Id(id, 0, typeName);
    }


    private String createNamespace(String clusterName) throws AtlasServiceException {
        System.out.println("Creating namespace entity");
        Referenceable hbaseNamespace = new Referenceable(HBASE_NAMESPACE_TYPE);
        hbaseNamespace.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default@" + clusterName);
        hbaseNamespace.set(AtlasClient.NAME, "default");
        hbaseNamespace.set(AtlasClient.DESCRIPTION, "Default HBase namespace");
        hbaseNamespace.set(AtlasClient.OWNER, "hbase_admin");
        String hbaseNamespaceJson = InstanceSerialization.toJson(hbaseNamespace, true);
        System.out.println(hbaseNamespaceJson);

        List<String> entitiesCreated = atlasClient.createEntity(hbaseNamespace);
        for (String entity : entitiesCreated) {
            System.out.println("Entity created: " + entity);
        }
        Utils.printDelimiter();
        return entitiesCreated.get(0);
    }
}
