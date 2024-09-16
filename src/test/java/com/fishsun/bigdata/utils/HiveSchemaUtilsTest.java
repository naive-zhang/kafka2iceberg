package com.fishsun.bigdata.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static javolution.testing.TestContext.assertNotNull;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/16 16:45
 * @Desc :
 */
public class HiveSchemaUtilsTest {
  @Test
  public void testGetTableSchema() {
    Configuration conf = new Configuration();
    conf.set("hive.metastore.uris", "thrift://localhost:9083");

    try {
      HiveSchemaUtils schemaUtil = new HiveSchemaUtils(conf);
      List<FieldSchema> schema = schemaUtil.getTableSchema("test", "t_busi_detail_flink_2");

      for (FieldSchema field : schema) {
        System.out.println("Column Name: " + field.getName());
        System.out.println("Column Type: " + field.getType());
        System.out.println("Comment: " + field.getComment());
        System.out.println("------------------------");
      }

      schemaUtil.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testGetTableParameters() {
    Configuration conf = new Configuration();
    conf.set("hive.metastore.uris", "thrift://localhost:9083");
    HiveSchemaUtils schemaUtil = null;
    try {
      schemaUtil = new HiveSchemaUtils(conf);
      Map<String, String> tableParameters = schemaUtil.getTableParameters("test", "t_busi_detail_flink_2");
//      System.out.println(schema);
      for (Map.Entry<String, String> kvSet : tableParameters.entrySet()) {
        System.out.println(String.format("%s  : %s", kvSet.getKey() , kvSet.getValue()));
      }
    } catch (TException e) {
      throw new RuntimeException(e);
    } finally {
      if (schemaUtil != null) {
        schemaUtil.close();
      }
    }
  }
}
