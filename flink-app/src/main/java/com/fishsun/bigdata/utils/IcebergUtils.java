package com.fishsun.bigdata.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;

import java.util.Map;

import static com.fishsun.bigdata.utils.ParamUtils.CATALOG_TYPE_KEY;
import static com.fishsun.bigdata.utils.ParamUtils.HADOOP_CATALOG;
import static com.fishsun.bigdata.utils.ParamUtils.HIVE_CATALOG;
import static com.fishsun.bigdata.utils.ParamUtils.ICEBERG_TABLE_LOCATION;
import static com.fishsun.bigdata.utils.ParamUtils.getHadoopConf;
import static com.fishsun.bigdata.utils.ParamUtils.getIcebergProps;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/10 10:00
 * @Desc :
 */
public class IcebergUtils {

  public static final String HIVE_CATALOG_NAME = "hive.catalog.name";
  public static final String HIVE_CATALOG_NS_NAME = "hive.namespace.name";
  public static final String HIVE_CATALOG_TBL_NAME = "hive.table.name";


  /**
   * 获得hadoop catalog tableLoader
   *
   * @param location
   * @param hadoopConf
   * @return
   */
  public static TableLoader hadoopLoader(
          String location
          , Configuration hadoopConf) {
    return TableLoader
            .fromHadoopTable(
                    location,
                    hadoopConf);
  }

  /**
   * 获得hive catalog下的tableLoader
   *
   * @param catalogName
   * @param icebergProps
   * @param hadoopConf
   * @param namespace
   * @param tableName
   * @return
   */
  public static TableLoader hiveLoader(
          String catalogName,
          Map<String, String> icebergProps,
          Configuration hadoopConf,
          String namespace,
          String tableName) {

    // Iceberg Catalog
    CatalogLoader catalogLoader = CatalogLoader
            .hive(catalogName, hadoopConf, icebergProps);
    System.out.println("in hiveLoader, catalogLoader init successfully");
    System.out.println(catalogLoader);

    // 定义 Iceberg 表标识
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace,
            tableName);
    System.out.println("in hiveLoader, tableIdentifier init successfully");
    System.out.println(tableIdentifier);

    // 加载 Iceberg 表
    TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);
    System.out.println("in hiveLoader, tableLoader init successfully");
    System.out.println(tableLoader);
    System.out.println("trying to init Catalog in hiveLoader");
    tableLoader.open();
    System.out.println("catalog init successfully");
    return tableLoader;
  }

  public static TableLoader getTableLoader(Map<String, String> paramMap) {
    if (paramMap.getOrDefault(CATALOG_TYPE_KEY, HIVE_CATALOG).equals(HADOOP_CATALOG)) {
      System.out.println("get table loader with hadoop catalog");
      return hadoopLoader(
              paramMap.get(ICEBERG_TABLE_LOCATION),
              getHadoopConf(paramMap)
      );
    }
    System.out.println("get table loader with hive catalog");
    return hiveLoader(
            paramMap.getOrDefault(HIVE_CATALOG_NAME, "iceberg_hive"),
            getIcebergProps(paramMap),
            getHadoopConf(paramMap),
            paramMap.getOrDefault(HIVE_CATALOG_NS_NAME, "test"),
            paramMap.getOrDefault(HIVE_CATALOG_TBL_NAME, "t_busi_detail_flink")
    );
  }
}
