package org.apache.atlas.demo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.Struct;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.json.TypesSerialization;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.utils.TypesUtil;

import java.util.HashMap;
import java.util.List;

public class AtlasTraitsDemo implements AtlasDemoConstants {

    private final AtlasClient atlasClient;

    public AtlasTraitsDemo(String atlasServiceUrl) {
        atlasClient = new AtlasClient(new String[]{atlasServiceUrl}, new String[]{"admin", "admin"});
    }

    public static void main(String[] args) throws AtlasServiceException {
        AtlasTraitsDemo atlasTraitsDemo = new AtlasTraitsDemo(args[0]);
        atlasTraitsDemo.run();
    }

    private void run() throws AtlasServiceException {
        createTraits();
        String tableGuid = getEntity(HBASE_TABLE_TYPE, AtlasEntitiesDemo.REMOTE_WEBTABLE_NAME);
        associateTrait(tableGuid);
    }

    private void associateTrait(String webTableGuid) throws AtlasServiceException {
        System.out.println("Adding trait " + PUBLIC_DATA_TRAIT_DEFINITION + " to: " + webTableGuid);
        Struct publicDataTraitInstance = new Struct(PUBLIC_DATA_TRAIT_DEFINITION);
        String traitJson = InstanceSerialization.toJson(publicDataTraitInstance, true);
        System.out.println(traitJson);
        atlasClient.addTrait(webTableGuid, publicDataTraitInstance);

        String contentsCfId = getEntity(HBASE_COLUMN_FAMILY_TYPE, AtlasEntitiesDemo.REMOTE_CONTENTS_CF_NAME);
        System.out.println("Adding trait " + RETAINABLE_TRAIT_DEFINITION + " to: " + contentsCfId);
        HashMap<String, Object> traitValues = new HashMap<>();
        traitValues.put(RETENTION_PERIOD_ATTRIBUTE_TYPE, 100);
        Struct retainableTraitInstance = new Struct(RETAINABLE_TRAIT_DEFINITION, traitValues);
        traitJson = InstanceSerialization.toJson(retainableTraitInstance, true);
        System.out.println(traitJson);
        atlasClient.addTrait(contentsCfId, retainableTraitInstance);
        Utils.printDelimiter();
    }

    private String getEntity(String hbaseColumnFamilyType, String remoteContentsCfName) throws AtlasServiceException {
        Referenceable contentsColumnFamily = atlasClient.getEntity(
                hbaseColumnFamilyType, AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
                remoteContentsCfName);
        return contentsColumnFamily.getId()._getId();
    }

    private void createTraits() throws AtlasServiceException {
        System.out.println("Creating traits...");
        HierarchicalTypeDefinition<TraitType> publicDataTrait =
                TypesUtil.createTraitTypeDef(PUBLIC_DATA_TRAIT_DEFINITION, ImmutableSet.<String>of());
        HierarchicalTypeDefinition<TraitType> retainableTrait
                = TypesUtil.createTraitTypeDef(RETAINABLE_TRAIT_DEFINITION, ImmutableSet.<String>of(),
                new AttributeDefinition(RETENTION_PERIOD_ATTRIBUTE_TYPE, DataTypes.INT_TYPE.getName(), Multiplicity.REQUIRED, false, null));

        TypesDef traitsTypeDefs = TypesUtil.getTypesDef(
                ImmutableList.<EnumTypeDefinition>of(),
                ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.of(publicDataTrait, retainableTrait),
                ImmutableList.<HierarchicalTypeDefinition<ClassType>>of());

        String traitsJson = TypesSerialization.toJson(traitsTypeDefs);
        System.out.println(traitsJson);

        List<String> traitsCreated = atlasClient.createType(traitsJson);
        for (String traitCreated : traitsCreated) {
            System.out.println("TraitCreated: " + traitCreated);
        }
        Utils.printDelimiter();
    }
}
