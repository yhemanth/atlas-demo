package org.apache.atlas.demo;

import org.apache.atlas.AtlasClient;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.demo.common.AtlasReferenceableBuilder;
import org.apache.atlas.demo.phoenix.Column;
import org.apache.atlas.demo.phoenix.PhoenixMetadata;
import org.apache.atlas.demo.phoenix.Table;
import org.apache.atlas.typesystem.Referenceable;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.atlas.demo.AtlasDemoConstants.*;

public class PhoenixMetadataImport {

    private static final String DEFAULT_OWNER = "admin";
    private final AtlasClient atlasClient;
    private final String clusterName;

    public PhoenixMetadataImport(String atlasUrl, String atlasUserName, String password, String clusterName) {
        this.clusterName = clusterName;
        atlasClient = new AtlasClient(new String[]{atlasUrl}, new String[]{atlasUserName, password});
    }

    public static void main(String[] args) throws SQLException, AtlasServiceException {
        if (args.length != 6) {
            printUsage();
            System.exit(-1);
        }
        PhoenixMetadataImport phoenixMetadataImport = new PhoenixMetadataImport(args[0], args[1], args[2], args[3]);
        phoenixMetadataImport.run(args[4], args[5]);
    }

    private void run(String phoenixHostName, String phoenixPort) throws SQLException, AtlasServiceException {
        PhoenixMetadataProfiler phoenixMetadataProfiler = new PhoenixMetadataProfiler(
                phoenixHostName, Integer.valueOf(phoenixPort));
        PhoenixMetadata phoenixMetadata = phoenixMetadataProfiler.getMetadata();
        addEntitiesToAtlas(phoenixMetadata);
    }

    private void addEntitiesToAtlas(PhoenixMetadata phoenixMetadata) throws AtlasServiceException {
        List<Table> tables = phoenixMetadata.getTables();
        for (Table t : tables) {
            System.out.println("Add/Update table: " + t.getName());
            List<Referenceable> columnReferenceables = getColumnReferenceables(t);

            Map<String, Object> tableProperties = new HashMap<>();
            tableProperties.put(PHOENIX_TABLE_ATTRIBUTE_COLUMNS, columnReferenceables);

            AtlasReferenceableBuilder atlasReferenceableBuilder = AtlasReferenceableBuilder.newAtlasReferenceableBuilder();
            Referenceable tableReferenceable = atlasReferenceableBuilder.ofType(PHOENIX_TABLE_TYPE).
                    withReferenceableName(String.format("%s@%s", t.getName(), clusterName)).
                    withAssetProperties(t.getName(), t.getDescription(), DEFAULT_OWNER).
                    withAttributeProperties(tableProperties).
                    build();

            List<Referenceable> allReferenceables = new ArrayList<>();
            allReferenceables.addAll(columnReferenceables);
            allReferenceables.add(tableReferenceable);
            atlasClient.updateEntities(allReferenceables);
        }
    }

    private List<Referenceable> getColumnReferenceables(Table t) {
        List<Column> columns = t.getColumns();
        List<Referenceable> columnReferenceables = new ArrayList<>();
        for (Column c : columns) {
            System.out.println("Creating entity reference for column: " + c.getName());
            Map<String, Object> columnProperties = new HashMap<>();
            columnProperties.put(PHOENIX_COLUMN_DATA_TYPE, c.getColumnType());
            columnProperties.put(PHOENIX_COLUMN_FAMILY_DATA_TYPE, c.getColumnFamily());
            System.out.println("Column family: " + c.getColumnFamily());

            AtlasReferenceableBuilder atlasReferenceableBuilder = AtlasReferenceableBuilder.newAtlasReferenceableBuilder();
            Referenceable phoenixColumn = atlasReferenceableBuilder.ofType(PHOENIX_COLUMN_TYPE).
                    withReferenceableName(String.format("%s.%s@%s", c.getName(), t.getName(), clusterName)).
                    withAssetProperties(c.getName(), c.getDescription(), DEFAULT_OWNER).
                    withAttributeProperties(columnProperties).
                    build();
            columnReferenceables.add(phoenixColumn);
        }
        return columnReferenceables;
    }

    private static void printUsage() {
        System.out.println("Usage: java org.apache.atlas.demo.PhoenixMetadataImport " +
                "<atlasUrl> <atlasUserName> <password> <clusterName> <phoenixHost> <phoenixPort>");
        System.out.println("Example: java org.apache.atlas.demo.PhoenixMetadataImport " +
                "http://localhost:21000/ admin PASSWORD cl1 localhost 8765");
    }
}
