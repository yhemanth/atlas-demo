package org.apache.atlas.demo;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.hook.AtlasHook;
import org.apache.atlas.notification.hook.HookNotification;
import org.apache.atlas.typesystem.Referenceable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AtlasHookDemo extends AtlasHook implements AtlasDemoConstants {

    public static final String CLUSTER_NAME = "cluster3";
    public static final String CONTENTS_CF_NAME = "default.webtable.contents@" + CLUSTER_NAME;
    public static final String WEBTABLE_NAME = "default.webtable@" + CLUSTER_NAME;
    public static final String KAFKA_USER_NAME = "integ_user";

    @Override
    protected String getNumberOfRetriesPropertyKey() {
        return "atlas.hook.demo.kafka.retries";
    }

    public static void main(String[] args) {
        AtlasHookDemo atlasHookDemo = new AtlasHookDemo();
        atlasHookDemo.run();
    }

    private void run() {
        createEntity();
        updateEntityInFull();
        updateEntityPartial();
        deleteEntity();
    }

    private void deleteEntity() {
        HookNotification.HookNotificationMessage message =
                new HookNotification.EntityDeleteRequest(KAFKA_USER_NAME, HBASE_TABLE_TYPE,
                        AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, WEBTABLE_NAME);
        notifyEntities(Arrays.asList(message));
    }

    private void updateEntityPartial() {
        Referenceable updatedTableEntity = new Referenceable(HBASE_TABLE_TYPE);
        updatedTableEntity.set(TABLE_ATTRIBUTE_IS_ENABLED, false);
        HookNotification.HookNotificationMessage message =
                new HookNotification.EntityPartialUpdateRequest(KAFKA_USER_NAME, HBASE_TABLE_TYPE,
                        AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME, WEBTABLE_NAME, updatedTableEntity);
        notifyEntities(Arrays.asList(message));
    }

    private void createEntity() {
        List<Referenceable> entities = new ArrayList<>();
        Referenceable namespace = Utils.createNamespace(CLUSTER_NAME);
        entities.add(namespace);
        HookNotification.HookNotificationMessage message
                = new HookNotification.EntityCreateRequest(KAFKA_USER_NAME, entities);
        notifyEntities(Arrays.asList(message));
    }

    private void updateEntityInFull() {
        List<Referenceable> entities = new ArrayList<>();
        Referenceable namespace = Utils.createNamespace(CLUSTER_NAME);
        entities.add(namespace);
        List<Referenceable> tableEntities = Utils.createTableEntities(CLUSTER_NAME, CONTENTS_CF_NAME, WEBTABLE_NAME,
                namespace.getId()._getId());
        entities.addAll(tableEntities);

        HookNotification.HookNotificationMessage message
                = new HookNotification.EntityUpdateRequest(KAFKA_USER_NAME, entities);
        notifyEntities(Arrays.asList(message));
    }

}
