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
    public void testHiveLoader() throws InterruptedException {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("default.FS", "hdfs://master1:9000");
        Map<String, String> icebergProps = new HashMap<>();
        icebergProps.put("uri", "thrift://hive:9083");
        icebergProps.put("warehouse", "hdfs://master1:9000/user/hive/warehouse");
        icebergProps.put("catalog-type", "hive");
        TableLoader tableLoader = IcebergUtils.hiveLoader(
                "iceberg_hive",
                icebergProps,
                hadoopConf,
                "test",
                "t_busi_detail_flink_2"
        );
        System.out.println(tableLoader);
    }
}
