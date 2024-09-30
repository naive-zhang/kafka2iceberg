package com.fishsun.bigdata.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import static com.fishsun.bigdata.utils.FieldUtils.fieldType2dataType;
import static com.fishsun.bigdata.utils.FieldUtils.fieldType2typeInformation;
import static com.fishsun.bigdata.utils.ParamUtils.ICEBERG_URI_KEY;

/**
 * A utility class for interacting with Hive Metastore and
 * converting Hive table schemas to Flink table schemas.
 * This class follows a thread-safe singleton pattern.
 */
public class HiveSchemaUtils implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(HiveSchemaUtils.class);
    private static HiveSchemaUtils instance;
    private static final Object lock = new Object();
    private IMetaStoreClient client;
    private Configuration conf;

    private IMetaStoreClient getClient() throws MetaException {
        if (client == null) {
            HiveConf hiveConf = new HiveConf();
            conf.forEach(
                    x-> hiveConf.set(x.getKey(), x.getValue())
            );
            this.client = new HiveMetaStoreClient(hiveConf);
        }
        return client;
    }

    private HiveSchemaUtils(Configuration conf) throws MetaException {
        this.conf = conf;
        this.getClient();
    }

    /**
     * Thread-safe singleton instance creator.
     *
     * @param paramMap Configuration parameters including Hive Metastore URI.
     * @return A single instance of HiveSchemaUtils.
     * @throws MetaException Thrown if an error occurs while creating the MetaStoreClient.
     */
    public static HiveSchemaUtils getInstance(Map<String, String> paramMap) throws MetaException {
        String thriftUri = paramMap.get(ICEBERG_URI_KEY);
        if (thriftUri == null || thriftUri.isEmpty()) {
            throw new IllegalArgumentException("Thrift URI cannot be null or empty");
        }
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("hive.metastore.uris", thriftUri);

        // Double-checked locking to ensure thread-safe singleton initialization.
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = new HiveSchemaUtils(hadoopConf);
                }
            }
        }
        return instance;
    }

    /**
     * Fetches the schema of a Hive table from the Metastore.
     *
     * @param databaseName The Hive database name.
     * @param tableName    The Hive table name.
     * @return A list of FieldSchema representing the table's schema.
     * @throws TException Thrown if an error occurs while retrieving the table schema.
     */
    public List<FieldSchema> getTableSchema(String databaseName, String tableName) throws TException {
        Table table = getClient().getTable(databaseName, tableName);
        return table.getSd().getCols();
    }

    /**
     * Retrieves the parameters of a specified Hive table.
     *
     * @param databaseName The Hive database name.
     * @param tableName    The Hive table name.
     * @return A map of table parameters.
     * @throws TException Thrown if an error occurs while retrieving the table parameters.
     */
    public Map<String, String> getTableParameters(String databaseName, String tableName) throws TException {
        Table table = getClient().getTable(databaseName, tableName);
        return table.getParameters();
    }

    public Set<String> getDateTypeFieldSet(String databaseName, String tableName) throws TException {
        List<FieldSchema> tableSchema = getTableSchema(
                databaseName,
                tableName
        );
        return tableSchema.stream()
                .filter(x -> x.getType().trim().equalsIgnoreCase("date"))
                .map(FieldSchema::getName)
                .collect(Collectors.toSet());
    }

    /**
     * 将Hive表的schema转化成Flink的ResolvedSchema，自动获取字段是否为空属性
     *
     * @param databaseName Hive数据库名
     * @param tableName    Hive表名
     * @return Flink ResolvedSchema 对象
     * @throws TException 如果获取Hive元数据时发生错误
     */
    public ResolvedSchema toFlinkResolvedSchema(String databaseName, String tableName) throws TException {
        // 1. 获取Hive表的schema信息
        Table hiveTable = getClient().getTable(databaseName, tableName);
        List<FieldSchema> tableSchema = hiveTable.getSd().getCols();

        // 2. 自动获取字段的非空属性
        Map<String, Boolean> fieldNullability = getFieldNullabilityFromMetaStore(hiveTable);
        List<String> primaryKeys = new LinkedList<>();

        // 3. 构建Flink的ResolvedSchema
        List<Column> columns = new ArrayList<>();
        for (FieldSchema fieldSchema : tableSchema) {
            String fieldName = fieldSchema.getName();
            DataType dataType = fieldType2dataType(fieldSchema.getType());

            // 如果字段是非空的，设置为 notNull
            if (Boolean.FALSE.equals(fieldNullability.get(fieldName))) {
                dataType = fieldType2dataType(fieldSchema.getType()).notNull();  // 表示该字段不可为空
                primaryKeys.add(fieldName);
            }

            Column.PhysicalColumn col = Column.physical(fieldName, dataType);
            columns.add(col);
        }

        return new ResolvedSchema(
                columns,
                Collections.emptyList(),
                primaryKeys.isEmpty() ? null : UniqueConstraint.primaryKey("primaryKey", primaryKeys)
        );
    }

    /**
     * Converts a Hive table schema to a Flink TableSchema.
     *
     * @param databaseName Hive database name.
     * @param tableName    Hive table name.
     * @return The corresponding Flink TableSchema.
     * @throws TException Thrown if an error occurs while retrieving the table schema.
     */
    public TableSchema toFlinkTableSchema(String databaseName, String tableName) throws TException {
        ResolvedSchema resolvedSchema = toFlinkResolvedSchema(databaseName, tableName);
        TableSchema tableSchema = TableSchema.fromResolvedSchema(resolvedSchema);
        logger.info("Converted TableSchema: {}", tableSchema);
        return tableSchema;
    }

    /**
     * Converts a Hive table schema to a Map of field names and their corresponding TypeInformation.
     *
     * @param databaseName Hive database name.
     * @param tableName    Hive table name.
     * @return A map of field names and their TypeInformation.
     * @throws TException Thrown if an error occurs while retrieving the table schema.
     */
    public Map<String, TypeInformation<?>> toFlinkFieldName2typeInformation(String databaseName, String tableName) throws TException {
        Map<String, TypeInformation<?>> fieldName2typeInfo = new HashMap<>();
        List<FieldSchema> tableSchema = getTableSchema(databaseName, tableName);

        for (FieldSchema fieldSchema : tableSchema) {
            fieldName2typeInfo.put(fieldSchema.getName(), fieldType2typeInformation(fieldSchema.getType()));
        }

        logger.info("Converted FieldName to TypeInformation Map: {}", fieldName2typeInfo);
        return fieldName2typeInfo;
    }

    /**
     * Converts a Hive table schema to a Flink RowTypeInfo.
     *
     * @param databaseName Hive database name.
     * @param tableName    Hive table name.
     * @return The corresponding Flink RowTypeInfo.
     * @throws TException Thrown if an error occurs while retrieving the table schema.
     */
    public TypeInformation<Row> toFlinkTypeInformation(String databaseName, String tableName) throws TException {
        List<FieldSchema> tableSchema = getTableSchema(databaseName, tableName);

        List<String> colNames = new ArrayList<>();
        List<TypeInformation<?>> typeInformationList = new ArrayList<>();
        for (FieldSchema fieldSchema : tableSchema) {
            colNames.add(fieldSchema.getName());
            typeInformationList.add(fieldType2typeInformation(fieldSchema.getType()));
        }

        String[] fieldNames = colNames.toArray(new String[0]);
        TypeInformation<?>[] fieldTypes = typeInformationList.toArray(new TypeInformation<?>[0]);

        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
        logger.info("Converted RowTypeInfo: {}", rowTypeInfo);
        return rowTypeInfo;
    }

    /**
     * Converts a Hive table schema to a Flink RowType.
     *
     * @param databaseName Hive database name.
     * @param tableName    Hive table name.
     * @return The corresponding Flink RowType.
     * @throws TException Thrown if an error occurs while retrieving the table schema.
     */
    public RowType toFlinkRowType(String databaseName, String tableName) throws TException {
        List<FieldSchema> tableSchema = getTableSchema(databaseName, tableName);
        Set<String> notNullColSet = getNotNullColSet(databaseName, tableName);
        List<String> fieldNames = new ArrayList<>();
        List<LogicalType> logicalTypes = new ArrayList<>();
        for (FieldSchema fieldSchema : tableSchema) {
            fieldNames.add(fieldSchema.getName());
            logicalTypes.add(FieldUtils.fieldType2logicalType(fieldSchema.getType(),
                    !notNullColSet.contains(fieldSchema.getName()))
            );
        }

        RowType rowType = RowType.of(logicalTypes.toArray(new LogicalType[0]), fieldNames.toArray(new String[0]));
        logger.info("Converted RowType: {}", rowType);
        return rowType;
    }

    /**
     * get a set filled with notnull-able field
     *
     * @param databaseName
     * @param tableName
     * @return
     * @throws TException
     */
    public Set<String> getNotNullColSet(String databaseName, String tableName) throws TException {
        Table table = getClient().getTable(databaseName, tableName);
        Map<String, Boolean> field2nullable = getFieldNullabilityFromMetaStore(table);
        return field2nullable.entrySet().stream()
                .filter(entry -> !entry.getValue())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * 从Hive Metastore中获取字段是否允许为空的信息。
     *
     * @param hiveTable Hive中的表对象
     * @return 字段名称和是否允许为空的映射表，true表示字段允许为空，false表示字段不允许为空
     */
    /**
     * 从Hive Metastore中获取字段是否允许为空的信息。如果字段是分区字段或者指定为NOT NULL，则不可为空。
     *
     * @param hiveTable Hive中的表对象
     * @return 字段名称和是否允许为空的映射表，true表示字段允许为空，false表示字段不允许为空
     */
    private Map<String, Boolean> getFieldNullabilityFromMetaStore(Table hiveTable) {

        String currentSchemaStr = hiveTable.getParameters().getOrDefault("current-schema", null);
        if (currentSchemaStr == null) {
            return hiveTable.getSd().getCols().stream()
                    .collect(Collectors.toMap(
                                    FieldSchema::getName,
                                    fieldSchema -> true
                            )
                    );
        } else {
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                JsonNode jsonNode = objectMapper.readTree(
                        currentSchemaStr.getBytes(StandardCharsets.UTF_8)
                );
                JsonNode fields = jsonNode.get("fields");
                if (fields != null && fields.isArray()) {
                    List<JsonNode> fieldList = new ArrayList<>();
                    for (JsonNode field : fields) {
                        fieldList.add(field);
                    }
                    return fieldList.stream()
                            .collect(
                                    Collectors.toMap(
                                            field -> field.get("name").asText(),
                                            field -> !field.get("required").asBoolean()
                                    )
                            );
                } else {
                    throw new IOException();
                }
            } catch (IOException e) {
                return hiveTable.getSd().getCols().stream()
                        .collect(Collectors.toMap(
                                        FieldSchema::getName,
                                        fieldSchema -> true
                                )
                        );
            }
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            logger.info("HiveMetaStoreClient closed successfully.");
            client = null;
        }
    }
}
