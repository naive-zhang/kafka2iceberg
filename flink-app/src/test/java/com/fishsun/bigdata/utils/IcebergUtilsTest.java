package com.fishsun.bigdata.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger logger = LoggerFactory.getLogger(IcebergUtilsTest.class);

  @Test
  public void testHiveLoader() throws InterruptedException {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("default.FS", "hdfs://mycluster");
    Map<String, String> icebergProps = new HashMap<>();
    icebergProps.put("uri", "thrift://bdtnode04:9083");
    icebergProps.put("warehouse", "hdfs://mycluster/user/hive/warehouse");
    icebergProps.put("catalog-type", "hive");
    TableLoader tableLoader = IcebergUtils.hiveLoader("iceberg_hive",
            icebergProps, hadoopConf, "ane_temp",
            "tx_waybill_info_iceberg");
    logger.info(tableLoader.toString());
  }
}
