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

    public static final String WEBTABLE_NAME = "default.webtable@cluster1";
    private final AtlasClient atlasClient;

    public AtlasEntitiesDemo(String atlasServiceUrl) {
        atlasClient = new AtlasClient(new String[]{atlasServiceUrl}, new String[]{"admin", "admin"});
    }

    public static void main(String[] args) throws AtlasServiceException, JSONException {
        AtlasEntitiesDemo atlasEntitiesDemo = new AtlasEntitiesDemo(args[0]);
        atlasEntitiesDemo.run();
    }

    private void run() throws AtlasServiceException, JSONException {
        String namespaceId = createNamespace();
        String tableId = createTable(namespaceId);
        retrieveEntity(namespaceId);
        retrieveEntity(tableId);
        retrieveEntityByUniqueAttribute(HBASE_TABLE_TYPE, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                WEBTABLE_NAME);
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

    private String createTable(String namespaceId) throws AtlasServiceException, JSONException {
        System.out.println("Creating Table, Column Family & Column entities");
        Referenceable cssNsiColumn = new Referenceable(HBASE_COLUMN_TYPE);
        cssNsiColumn.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default.webtable.anchor.cssnsi@cluster1");
        cssNsiColumn.set(AtlasClient.NAME, "cssnsi");
        cssNsiColumn.set(AtlasClient.OWNER, "crawler");
        cssNsiColumn.set(COLUMN_ATTRIBUTE_TYPE, "string");

        Referenceable myLookCaColumn = new Referenceable(HBASE_COLUMN_TYPE);
        myLookCaColumn.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default.webtable.anchor.mylookca@cluster1");
        myLookCaColumn.set(AtlasClient.NAME, "mylookca");
        myLookCaColumn.set(AtlasClient.OWNER, "crawler");
        myLookCaColumn.set(COLUMN_ATTRIBUTE_TYPE, "string");

        Referenceable htmlColumn = new Referenceable(HBASE_COLUMN_TYPE);
        htmlColumn.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default.webtable.contents.html@cluster1");
        htmlColumn.set(AtlasClient.NAME, "html");
        htmlColumn.set(AtlasClient.OWNER, "crawler");
        htmlColumn.set(COLUMN_ATTRIBUTE_TYPE, "byte[]");

        Referenceable anchorCf = new Referenceable(HBASE_COLUMN_FAMILY_TYPE);
        anchorCf.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default.webtable.anchor@cluster1");
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
        contentsCf.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default.webtable.contents@cluster1");
        contentsCf.set(AtlasClient.NAME, "contents");
        contentsCf.set(AtlasClient.DESCRIPTION, "The contents column family that stores the crawled content");
        contentsCf.set(AtlasClient.OWNER, "crawler");
        contentsCf.set(CF_ATTRIBUTE_VERSIONS, 1);
        contentsCf.set(CF_ATTRIBUTE_IN_MEMORY, false);
        contentsCf.set(CF_ATTRIBUTE_COMPRESSION, "lzo");
        contentsCf.set(CF_ATTRIBUTE_BLOCK_SIZE, 1024);
        contentsCf.set(CF_ATTRIBUTE_COLUMNS, Arrays.asList(getReferenceableId(htmlColumn)));

        Referenceable webTable = new Referenceable(HBASE_TABLE_TYPE);
        webTable.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, WEBTABLE_NAME);
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


    private String createNamespace() throws AtlasServiceException {
        System.out.println("Creating namespace entity");
        Referenceable hbaseNamespace = new Referenceable(HBASE_NAMESPACE_TYPE);
        hbaseNamespace.set(AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, "default@cluster1");
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
