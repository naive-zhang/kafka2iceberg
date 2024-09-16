package com.fishsun.bigdata.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.fishsun.bigdata.utils.FieldUtils.fieldType2dataType;
import static com.fishsun.bigdata.utils.FieldUtils.fieldType2typeInformation;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/10 10:36
 * @Desc :
 */
public class ParamUtils {

  private static final Logger logger = LoggerFactory.getLogger(ParamUtils.class);

  public static final String ICEBERG_TABLE_LOCATION = "iceberg.table.location";
  public static final String CATALOG_TYPE_KEY = "iceberg.catalog.type";
  public static final String HADOOP_CATALOG = "hadoop";
  public static final String HIVE_CATALOG = "hive";

  public static final String DEFAULT_FS_KEY = "default.FS";

  public static final String ICEBERG_URI_KEY = "iceberg.uri";

  public static final String ICEBERG_WAREHOUSE_KEY = "iceberg.warehouse";

  public static final String ICEBERG_HIVE_CATALOG_KEY = "iceberg.catalog-type";

  public static final String ICEBERG_HIVE_CATALOG_VALUE = "hive";

  public static Map<String, String> parseConfig(String[] args) {
    Map<String, String> paramMap = new HashMap<>();
    for (String arg : args) {
      if (arg.contains("=")) {
        String[] tmpParams = arg.split("=");
        String paramName = tmpParams[0];
        if (paramName.startsWith("'") || paramName.startsWith("\"")) {
          paramName = paramName.substring(1);
        }
        String[] paramValList = new String[tmpParams.length - 1];
        for (int i = 0; i < paramValList.length; i++) {
          paramValList[i] = tmpParams[i + 1];
        }
        String paramVal = String.join("=", paramValList);
        if (paramVal.endsWith("'") || paramName.endsWith("\"")) {
          paramVal = paramVal.substring(0, paramVal.length()-1);
        }
        paramMap.put(paramName, paramVal);
      }
    }
    paramMap.putIfAbsent("host", "db");
    paramMap.putIfAbsent("port", "3306");
    paramMap.putIfAbsent("user", "root");
    paramMap.putIfAbsent("password", "123456");
    return paramMap;
  }

  public static void enhanceConfig(Map<String, String> paramMap) {
    // 用于产生hadoopConf
    if (paramMap.getOrDefault(DEFAULT_FS_KEY, null) == null) {
      paramMap.put(DEFAULT_FS_KEY, "hdfs://master1:9000");
    }
    if (paramMap.getOrDefault(
            CATALOG_TYPE_KEY, null
    ) == null) {
      paramMap.put(CATALOG_TYPE_KEY, HADOOP_CATALOG);
    }
    // 用于产生icebergProps
    if (paramMap.getOrDefault(ICEBERG_URI_KEY, null) == null) {
      paramMap.put(ICEBERG_URI_KEY, "thrift://hive:9083");
    }
    if (paramMap.getOrDefault(ICEBERG_WAREHOUSE_KEY, null) == null) {
      paramMap.put(ICEBERG_WAREHOUSE_KEY, "hdfs://master1:9000/user/hive/warehouse");
    }
    if (paramMap.getOrDefault(ICEBERG_HIVE_CATALOG_KEY, null) == null) {
      paramMap.put(ICEBERG_HIVE_CATALOG_KEY, ICEBERG_HIVE_CATALOG_VALUE);
    }
    // 默认的表
    if (paramMap.getOrDefault(ICEBERG_TABLE_LOCATION, null) == null) {
      paramMap.put(ICEBERG_TABLE_LOCATION, "hdfs://master1:9000/user/hive/warehouse/test.db/t_busi_detail");
    }

    System.out.println("expand enhancedParamMap");
    logger.info("expand enhanced params map");
    for (Map.Entry<String, String> kv : paramMap.entrySet()) {
      logger.info("config [{}]  : {}", kv.getKey(), kv.getValue());
    }
  }

  public static Map<String, String> getIcebergProps(Map<String, String> paramMap) {
    Map<String, String> icebergProps = new HashMap<>();
    for (Map.Entry<String, String> kv : paramMap.entrySet()) {
      if (kv.getKey().startsWith("iceberg.")) {
        icebergProps.put(kv.getKey().replace("iceberg.", ""),
                kv.getValue());
      }
    }
    return icebergProps;
  }

  public static Configuration getHadoopConf(Map<String, String> paramMap) {
    if (paramMap.getOrDefault(DEFAULT_FS_KEY, null) == null) {
      enhanceConfig(paramMap);
    }
    Configuration hadoopConf = new Configuration();
    hadoopConf.set(DEFAULT_FS_KEY, paramMap.get(DEFAULT_FS_KEY));
    return hadoopConf;
  }


  public static Map<Integer, Map<String, String>> filterTableSchemaConfig(Map<String, String> paramMap) {
    Map<Integer, Map<String, String>> seq2fieldInfo = new HashMap<>();
    Map<String, Integer> name2seq = new HashMap<>();
    for (Map.Entry<String, String> kv : paramMap.entrySet()) {
      String key = kv.getKey();
      String value = kv.getValue();
      if (!key.startsWith("fields.")) {
        continue;
      }
      key = key.substring(7);
      String[] split = key.split("\\.");
      String fieldName = split[0];
      String fieldKey = split[1];
      if (fieldKey.trim().equalsIgnoreCase("seq")) {
        Map<String, String> fieldInfo = seq2fieldInfo.getOrDefault(Integer.valueOf(value), new HashMap<String, String>());
        // fields.bid.seq = 1
        // fields.bid.type = bigint
        // fields.bid.is_nullable = false
        // fields.bid.is_primary_key = true
        // fields.bid.ref = data.bid
        fieldInfo.put("name", fieldName);
        fieldInfo.put("seq", value);
        seq2fieldInfo.put(Integer.valueOf(value), fieldInfo);
        name2seq.put(fieldName, Integer.valueOf(value));
      }
    }
    for (Map.Entry<String, String> kv : paramMap.entrySet()) {
      String key = kv.getKey();
      String value = kv.getValue();
      if (!key.startsWith("fields.")) {
        continue;
      }
      key = key.substring(7);
      String[] split = key.split("\\.");
      String fieldName = split[0];
//      if (fieldName.equalsIgnoreCase("fee")) {
//        System.out.println(value);
//      }
      String fieldKey = split[1];
      Integer seq = name2seq.get(fieldName);
      Map<String, String> fieldInfo = seq2fieldInfo.get(seq);
      fieldInfo.put(fieldKey.trim().toLowerCase(), value.trim().toLowerCase());
    }
    for (Map<String, String> fieldInfo : seq2fieldInfo.values()) {
      if (fieldInfo.getOrDefault("is_nullable", null) == null) {
        fieldInfo.put("is_nullable", "true");
      }
      if (fieldInfo.getOrDefault("is_primary_key", null) == null) {
        fieldInfo.put("is_primary_key", "false");
      }
      if (fieldInfo.getOrDefault("ref", null) == null) {
        fieldInfo.put("ref", "data." + fieldInfo.get("name"));
      }
    }
    return seq2fieldInfo;
  }

  public static TableSchema parseTableSchema(Map<String, String> paramMap) {
    Map<Integer, Map<String, String>> seq2fieldInfo = filterTableSchemaConfig(paramMap);
    List<Column> cols = new LinkedList<>();
    List<String> primaryKeys = new LinkedList<>();
    for (int i = 0; i < seq2fieldInfo.size(); i++) {
      Map<String, String> fieldInfo = seq2fieldInfo.get(i + 1);
      String fieldName = fieldInfo.get("name");
      String type = fieldInfo.get("type");
      boolean isNullable = fieldInfo.get("is_nullable").equalsIgnoreCase("true");
      boolean isPrimaryKey = fieldInfo.get("is_primary_key").equalsIgnoreCase("true");
      Column.PhysicalColumn col = Column.PhysicalColumn.physical(
              fieldName,
              isNullable ? fieldType2dataType(type) : fieldType2dataType(type).notNull()
      );
      // 是否主键
      if (isPrimaryKey) {
        primaryKeys.add(fieldName);
      }
      cols.add(col);
    }
    UniqueConstraint primaryKey = UniqueConstraint.primaryKey("primaryKey", primaryKeys);
    TableSchema tableSchema = TableSchema.fromResolvedSchema(
            new ResolvedSchema(cols,
                    Collections.emptyList(),
                    primaryKeys.isEmpty() ? null : primaryKey
            )
    );
    logger.info("parsed table schema: {}", tableSchema.toString());
    System.out.println(tableSchema.toString());
    return tableSchema;
  }

  public static TypeInformation<Row> parseTypeInformation(Map<String, String> paramMap) {

    Map<Integer, Map<String, String>> seq2fieldInfo = filterTableSchemaConfig(paramMap);
    List<String> colNames = new LinkedList<>();
    List<TypeInformation<?>> typeInformationList = new LinkedList<>();
    for (int i = 0; i < seq2fieldInfo.size(); i++) {
      Map<String, String> fieldInfo = seq2fieldInfo.get(i + 1);
      colNames.add(fieldInfo.get("name"));
      typeInformationList.add(fieldType2typeInformation(fieldInfo.get("type")));
    }
    TypeInformation<?>[] typeInformations = new TypeInformation[typeInformationList.size()];
    for (int i = 0; i < typeInformationList.size(); i++) {
      typeInformations[i] = typeInformationList.get(i);
    }
    String[] fieldNames = new String[colNames.size()];
    TypeInformation<?>[] fieldTypes = new TypeInformation<?>[colNames.size()];
    for (int i = 0; i < fieldNames.length; i++) {
      fieldNames[i] = colNames.get(i);
      fieldTypes[i] = typeInformationList.get(i);
    }
    RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);
    logger.info("parsed type information: {}", rowTypeInfo.toString());
    System.out.println(rowTypeInfo.toString());
    return rowTypeInfo;
  }


  public static void main(String[] args) {
    Map<String, String> paramMap = parseConfig(args);
    enhanceConfig(paramMap);
    Map<Integer, Map<String, String>> integerMapMap = filterTableSchemaConfig(paramMap);
    for (Map.Entry<Integer, Map<String, String>> seq2fieldInfo : integerMapMap.entrySet()) {
//      System.out.println("seq==============" + seq2fieldInfo.getKey());
      for (Map.Entry<String, String> stringStringEntry : seq2fieldInfo.getValue().entrySet()) {
//        System.out.println(stringStringEntry.getKey() + " : " + stringStringEntry.getValue());
      }
    }
    TableSchema tableSchema = parseTableSchema(
            paramMap
    );
    System.out.println(tableSchema.toString());
    TypeInformation<Row> rowTypeInformation = parseTypeInformation(
            paramMap
    );
    System.out.println("row type information");
    System.out.println(rowTypeInformation);
  }
}
