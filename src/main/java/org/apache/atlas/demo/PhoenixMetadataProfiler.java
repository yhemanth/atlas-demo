package org.apache.atlas.demo;

import org.apache.atlas.demo.phoenix.Column;
import org.apache.atlas.demo.phoenix.PhoenixMetadata;
import org.apache.atlas.demo.phoenix.Table;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PhoenixMetadataProfiler {
    static {
        try {
            Class.forName("org.apache.phoenix.queryserver.client.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private final Connection connection;

    public PhoenixMetadataProfiler(String host, int port) throws SQLException {
        String url = String.format("jdbc:phoenix:thin:url=http://%s:%d;serialization=PROTOBUF", host, port);
        connection = DriverManager.getConnection(url);
    }

    public static void main(String[] args) throws SQLException {
        PhoenixMetadataProfiler phoenixMetadataProfiler =
                new PhoenixMetadataProfiler("hyamijala-dp-fenton-dev-1.openstacklocal", 8765);
        phoenixMetadataProfiler.run();
    }

    private void run() throws SQLException {
        PhoenixMetadata metadata = getMetadata();
        for (Table t: metadata.getTables()) {
            System.out.println("Table: " + t.getName());
            for (Column c : t.getColumns()) {
                String columnDetails = String.format("%s\t%s\t%s", c.getName(), c.getColumnType(), c.getColumnFamily());
                System.out.println(columnDetails);
            }
        }
    }

    public PhoenixMetadata getMetadata() throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tables = metaData.getTables("", null, null, new String[]{"TABLE"});
        PhoenixMetadata phoenixMetadata = new PhoenixMetadata();
        while (tables.next()) {
            String tableName = tables.getString(3);
            Table table = new Table(tableName, tableName);
            ResultSet columns = metaData.getColumns("", null, tableName, null);
            while (columns.next()) {
                String columnName = columns.getString(4);
                Column column = new Column(columnName, columnName,
                        columns.getString(6), columns.getString("COLUMN_FAMILY"));
                table.addColumn(column);
            }
            phoenixMetadata.add(table);
        }
        connection.close();
        return phoenixMetadata;
    }
}
