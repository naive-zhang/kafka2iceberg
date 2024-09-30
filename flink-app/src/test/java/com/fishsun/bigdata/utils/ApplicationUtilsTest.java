package com.fishsun.bigdata.utils;

import org.apache.iceberg.flink.TableLoader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/30 18:16
 * @Desc :
 */
public class ApplicationUtilsTest {
  private static final Logger logger = LoggerFactory.getLogger(HiveSchemaUtilsTest.class);
  private static final String DATABASE_NAME = "ane_temp";
  private static final String TABLE_NAME = "tx_waybill_info_iceberg";
  private static final String argString = "iceberg.catalog.type=hive iceberg.uri=thrift://bdtnode04:9083 hive.catalog.name=hive_iceberg hive.namespace.name=ane_temp hive.table.name=tx_waybill_info_iceberg bootstrap.servers=kafka:9092 topics=example group.id=flink-group source-database=test source-table=t_busi_detail fields.bid.is_primary_key=true fields.dt.is_primary_key=true fields.dt.ref=data.create_time";
  private static String[] args;
  private static Map<String, String> paramMap;

  @BeforeAll
  public static void setup() {
    args = argString.split("\\s+");
    paramMap = ParamUtils.parseConfig(args);
    ParamUtils.enhanceConfig(paramMap);
  }

  @Test
  public void testGetTableLoader() throws InterruptedException {
    TableLoader tableLoader = ApplicationUtils.setupTableLoader(paramMap);
    System.out.println(tableLoader);
  }
}
