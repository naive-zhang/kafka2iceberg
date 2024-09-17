package com.fishsun.bigdata.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/17 16:50
 * @Desc :
 */
public class IcebergUtilsTest {
  @Test
  public void testHiveLoader() {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("default.FS", "hdfs://master1:9000");
    Map<String, String> icebergProps = new HashMap<>();
    icebergProps.put("uri", "thrift://hive:9083");
    icebergProps.put("warehouse", "hdfs://master1:9000/user/hive/warehouse");
    icebergProps.put("catalog-type", "hive");
    // Iceberg Catalog
    CatalogLoader catalogLoader = CatalogLoader
            .hive("iceberg_hive", hadoopConf, icebergProps);

    // 定义 Iceberg 表标识
    TableIdentifier tableIdentifier = TableIdentifier.of("test",
            "t_busi_detail_flink_2");

    // 加载 Iceberg 表
    TableLoader.fromCatalog(catalogLoader, tableIdentifier);
    TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);
    tableLoader.open();
//    Catalog catalog = catalogLoader.loadCatalog();
//    Table table1 = catalog.loadTable(tableIdentifier);
    Table table = tableLoader.loadTable();
    Assertions.assertTrue(tableLoader!=null);
  }
}
