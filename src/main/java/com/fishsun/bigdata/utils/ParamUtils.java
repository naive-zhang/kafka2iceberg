package com.fishsun.bigdata.utils;

import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/10 10:36
 * @Desc :
 */
public class ParamUtils {
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
        String[] paramValList = new String[tmpParams.length - 1];
        for (int i = 0; i < paramValList.length; i++) {
          paramValList[i] = tmpParams[i + 1];
        }
        String paramVal = String.join("=", paramValList);
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
    for (Map.Entry<String, String> kv : paramMap.entrySet()) {
      System.out.println(String.format("{%s} {%s}", kv.getKey(), kv.getValue()));
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
}
