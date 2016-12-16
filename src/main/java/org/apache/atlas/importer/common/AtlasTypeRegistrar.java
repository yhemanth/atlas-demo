package org.apache.atlas.importer.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.typesystem.TypesDef;
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

import java.util.List;

public class AtlasTypeRegistrar implements AtlasTypeConstants {

    private final AtlasClient atlasClient;

    public AtlasTypeRegistrar(String atlasServiceUrl) {
        atlasClient = new AtlasClient(new String[]{atlasServiceUrl}, new String[]{"admin", "admin"});
    }

    public static void main(String[] args) throws AtlasServiceException {
        AtlasTypeRegistrar atlasTypeRegistrar = new AtlasTypeRegistrar(args[0]);
        atlasTypeRegistrar.run();
    }

    private void run() throws AtlasServiceException {
        List<String> types = listTypes();
        listAType();
        createNewTypes(types);
        listTypes();
    }

    private void listAType() throws AtlasServiceException {
        System.out.println("Printing type definition for type: " + AtlasClient.ASSET_TYPE);
        TypesDef type = atlasClient.getType(AtlasClient.ASSET_TYPE);
        String typeJson = TypesSerialization.toJson(type);
        System.out.println("Type definition for type: " + AtlasClient.ASSET_TYPE);
        System.out.println(typeJson);
        printDelimiter();
    }

    private List<String> listTypes() throws AtlasServiceException {
        System.out.println("Types registered with Atlas:");
        List<String> types = atlasClient.listTypes();
        for (String type : types) {
            System.out.println("Type: " + type);
        }
        printDelimiter();
        return types;
    }

    private void createNewTypes(List<String> types) throws AtlasServiceException {
        System.out.println("Creating new types");
        createHBaseTypes(types);
        createPhoenixTypes(types);

    }

    private void createPhoenixTypes(List<String> types) throws AtlasServiceException {
        if (!types.contains(PHOENIX_TABLE_TYPE)) {
            HierarchicalTypeDefinition<ClassType> columnType =
                    TypesUtil.createClassTypeDef(PHOENIX_COLUMN_TYPE, ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE, AtlasClient.ASSET_TYPE),
                            new AttributeDefinition(PHOENIX_COLUMN_DATA_TYPE, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                            new AttributeDefinition(PHOENIX_COLUMN_FAMILY_DATA_TYPE, DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null));
            HierarchicalTypeDefinition<ClassType> tableType =
                    TypesUtil.createClassTypeDef(PHOENIX_TABLE_TYPE, ImmutableSet.of(AtlasClient.DATA_SET_SUPER_TYPE),
                            new AttributeDefinition(PHOENIX_TABLE_ATTRIBUTE_COLUMNS, DataTypes.arrayTypeName(PHOENIX_COLUMN_TYPE), Multiplicity.COLLECTION, true, null));

            TypesDef phoenixTypes = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                    ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                    ImmutableList.of(columnType, tableType));
            addTypes(phoenixTypes);
        }
    }

    private void createHBaseTypes(List<String> types) throws AtlasServiceException {
        if (!types.contains(HBASE_NAMESPACE_TYPE)) {
            HierarchicalTypeDefinition<ClassType> namespaceType =
                    TypesUtil.createClassTypeDef(HBASE_NAMESPACE_TYPE, ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE, AtlasClient.ASSET_TYPE));
            HierarchicalTypeDefinition<ClassType> columnFamilyType =
                    TypesUtil.createClassTypeDef(HBASE_COLUMN_FAMILY_TYPE, ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE, AtlasClient.ASSET_TYPE),
                        new AttributeDefinition(CF_ATTRIBUTE_VERSIONS, DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                        new AttributeDefinition(CF_ATTRIBUTE_IN_MEMORY, DataTypes.BOOLEAN_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                        new AttributeDefinition(CF_ATTRIBUTE_BLOCK_SIZE, DataTypes.INT_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition(CF_ATTRIBUTE_COMPRESSION, DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null));
            HierarchicalTypeDefinition<ClassType> tableType =
                    // In older builds, there was no Asset type, and DataSet was not extending Asset. If used with those
                    // builds, we need to define both DataSet and Asset as supertypes.
                     TypesUtil.createClassTypeDef(HBASE_TABLE_TYPE, ImmutableSet.of(AtlasClient.DATA_SET_SUPER_TYPE),
                        new AttributeDefinition(TABLE_ATTRIBUTE_NAMESPACE, HBASE_NAMESPACE_TYPE, Multiplicity.REQUIRED, false, null),
                        new AttributeDefinition(TABLE_ATTRIBUTE_IS_ENABLED, DataTypes.BOOLEAN_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                        new AttributeDefinition(TABLE_ATTRIBUTE_COLUMN_FAMILIES, DataTypes.arrayTypeName(HBASE_COLUMN_FAMILY_TYPE), Multiplicity.COLLECTION, true, null));
            TypesDef hbaseTypes = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                    ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                    ImmutableList.of(namespaceType, columnFamilyType, tableType));
            addTypes(hbaseTypes);
        }
    }

    private void addTypes(TypesDef types) throws AtlasServiceException {
        String typesAsString = TypesSerialization.toJson(types);
        System.out.println(typesAsString);
        List<String> typesCreated = atlasClient.createType(types);
        for (String typeCreated : typesCreated) {
            System.out.println("TypeCreated: " + typeCreated);
        }
        printDelimiter();
    }

    public static void printDelimiter() {
        System.out.println("============================================");
    }
}
