package org.apache.atlas.demo;

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

public class AtlasTypesDemo {

    public static final String HBASE_NAMESPACE_TYPE = "hbase_namespace";
    public static final String HBASE_TABLE_TYPE = "hbase_table";
    public static final String HBASE_COLUMN_TYPE = "hbase_column";
    public static final String HBASE_COLUMN_FAMILY_TYPE = "hbase_column_family";
    private final AtlasClient atlasClient;

    public AtlasTypesDemo(String atlasServiceUrl) {
        atlasClient = new AtlasClient(new String[]{atlasServiceUrl}, new String[]{"admin", "admin"});
    }

    public static void main(String[] args) throws AtlasServiceException {
        AtlasTypesDemo atlasTypesDemo = new AtlasTypesDemo(args[0]);
        atlasTypesDemo.run();
    }

    private void run() throws AtlasServiceException {
        listTypes();
        listAType();
        createNewTypes();
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

    private void listTypes() throws AtlasServiceException {
        System.out.println("Types registered with Atlas:");
        List<String> types = atlasClient.listTypes();
        for (String type : types) {
            System.out.println("Type: " + type);
        }
        printDelimiter();
    }

    private void printDelimiter() {
        System.out.println("============================================");
    }

    private void createNewTypes() throws AtlasServiceException {
        System.out.println("Creating new types");
        HierarchicalTypeDefinition<ClassType> namespaceType =
                TypesUtil.createClassTypeDef(HBASE_NAMESPACE_TYPE, ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE, AtlasClient.ASSET_TYPE));
        HierarchicalTypeDefinition<ClassType> columnType =
                TypesUtil.createClassTypeDef(HBASE_COLUMN_TYPE, ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE, AtlasClient.ASSET_TYPE),
                    new AttributeDefinition("type", DataTypes.STRING_TYPE.getName(), Multiplicity.REQUIRED, false, null));
        HierarchicalTypeDefinition<ClassType> columnFamilyType =
                TypesUtil.createClassTypeDef(HBASE_COLUMN_FAMILY_TYPE, ImmutableSet.of(AtlasClient.REFERENCEABLE_SUPER_TYPE, AtlasClient.ASSET_TYPE),
                    new AttributeDefinition("versions", DataTypes.INT_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                    new AttributeDefinition("inMemory", DataTypes.BOOLEAN_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                    new AttributeDefinition("blockSize", DataTypes.INT_TYPE.getName(), Multiplicity.REQUIRED, false, null),
                    new AttributeDefinition("compression", DataTypes.STRING_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                    new AttributeDefinition("columns", DataTypes.arrayTypeName(HBASE_COLUMN_TYPE), Multiplicity.COLLECTION, false, null));
        HierarchicalTypeDefinition<ClassType> tableType =
                // In older builds, there was no Asset type, and DataSet was not extending Asset. If used with those
                // builds, we need to define both DataSet and Asset as supertypes.
                // TypesUtil.createClassTypeDef(HBASE_TABLE_TYPE, ImmutableSet.of(AtlasClient.DATA_SET_SUPER_TYPE),
                TypesUtil.createClassTypeDef(HBASE_TABLE_TYPE, ImmutableSet.of(AtlasClient.DATA_SET_SUPER_TYPE, AtlasClient.ASSET_TYPE),
                    new AttributeDefinition("namespace", HBASE_NAMESPACE_TYPE, Multiplicity.REQUIRED, false, null),
                    new AttributeDefinition("status", DataTypes.BOOLEAN_TYPE.getName(), Multiplicity.OPTIONAL, false, null),
                    new AttributeDefinition("columnFamilies", DataTypes.arrayTypeName(HBASE_COLUMN_FAMILY_TYPE), Multiplicity.COLLECTION, true, null));
        TypesDef hbaseTypes = TypesUtil.getTypesDef(ImmutableList.<EnumTypeDefinition>of(), ImmutableList.<StructTypeDefinition>of(),
                ImmutableList.<HierarchicalTypeDefinition<TraitType>>of(),
                ImmutableList.of(namespaceType, columnType, columnFamilyType, tableType));
        String typesAsString = TypesSerialization.toJson(hbaseTypes);
        System.out.println(typesAsString);

        List<String> typesCreated = atlasClient.createType(hbaseTypes);
        for (String typeCreated : typesCreated) {
            System.out.println("TypeCreated: " + typeCreated);
        }
        printDelimiter();
    }
}
