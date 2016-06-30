package org.apache.atlas.demo;

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
        createEntities();
    }

    private void createEntities() {
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
