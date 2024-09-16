package com.fishsun.bigdata.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/16 16:43
 * @Desc :
 */
public class HiveSchemaUtils {

  private IMetaStoreClient client;

  public HiveSchemaUtils(Configuration conf) throws MetaException {
    this.client = new HiveMetaStoreClient(conf);
  }

  public List<FieldSchema> getTableSchema(String databaseName, String tableName) throws TException {
    Table table = client.getTable(databaseName, tableName);
    return table.getSd().getCols();
  }

  /**
   * 获取指定表的参数 Map
   *
   * @param databaseName 数据库名
   * @param tableName    表名
   * @return 表的参数 Map
   * @throws TException
   */
  public Map<String, String> getTableParameters(String databaseName, String tableName) throws TException {
    Table table = client.getTable(databaseName, tableName);
    return table.getParameters();
  }

  /**
   * 获取指定表的指定参数值
   *
   * @param databaseName 数据库名
   * @param tableName    表名
   * @param paramKey     参数键
   * @return 参数值
   * @throws TException
   */
  public String getTableParameter(String databaseName, String tableName, String paramKey) throws TException {
    Map<String, String> params = getTableParameters(databaseName, tableName);
    return params.get(paramKey);
  }

  public void close() {
    if (client != null) {
      client.close();
    }
  }
}