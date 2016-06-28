package org.apache.atlas.demo;

public interface AtlasDemoConstants {
    String HBASE_NAMESPACE_TYPE = "hbase_namespace";
    String HBASE_TABLE_TYPE = "hbase_table";
    String HBASE_COLUMN_TYPE = "hbase_column";
    String HBASE_COLUMN_FAMILY_TYPE = "hbase_column_family";
    String HBASE_REPLICATION_PROCESS_TYPE = "hbase_replication_process";

    String COLUMN_ATTRIBUTE_TYPE = "type";
    String CF_ATTRIBUTE_VERSIONS = "versions";
    String CF_ATTRIBUTE_IN_MEMORY = "inMemory";
    String CF_ATTRIBUTE_BLOCK_SIZE = "blockSize";
    String CF_ATTRIBUTE_COMPRESSION = "compression";
    String CF_ATTRIBUTE_COLUMNS = "columns";
    String TABLE_ATTRIBUTE_NAMESPACE = "namespace";
    String TABLE_ATTRIBUTE_IS_ENABLED = "isEnabled";
    String TABLE_ATTRIBUTE_COLUMN_FAMILIES = "columnFamilies";
    String REPLICATION_SCHEDULE = "replicationSchedule";
    String REPLICATION_ENABLED = "replicationEnabled";

    String PUBLIC_DATA_TRAIT_DEFINITION = "PublicData";
    String RETAINABLE_TRAIT_DEFINITION = "Retainable";
    String RETENTION_PERIOD_ATTRIBUTE_TYPE = "retentionPeriod";
}
