package com.fishsun.bigdata.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.fishsun.bigdata.utils.FieldUtils.fieldType2dataType;
import static com.fishsun.bigdata.utils.FieldUtils.fieldType2typeInformation;

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


  /**
   * 将某个存储在hive catalog里的表转化成flink resolved schema
   *
   * @param databaseName
   * @param tableName
   * @param notnullFieldList
   * @param primaryKeys
   * @return
   * @throws TException
   */
  public ResolvedSchema toFlinkResolvedSchema(String databaseName, String tableName, List<String> notnullFieldList, List<String> primaryKeys) throws TException {
    // 先获得hive表的schema
    List<FieldSchema> tableSchema = getTableSchema(databaseName, tableName);
    // 做两个Set快速查询
    Set<String> notNullFieldSet = new HashSet<>();
    Set<String> primarySet = new HashSet<>();
    if (notnullFieldList != null && !notnullFieldList.isEmpty()) {
      notNullFieldSet.addAll(notnullFieldList);
    }
    if (primaryKeys != null && !primaryKeys.isEmpty()) {
      primarySet.addAll(primaryKeys);
      notNullFieldSet.addAll(primaryKeys);
    }
    // 将schema进行转换
    List<Column> cols = new LinkedList<>();
    for (int i = 0; i < tableSchema.size(); i++) {
      FieldSchema fieldSchema = tableSchema.get(i);
      String fieldName = fieldSchema.getName();
      String type = fieldSchema.getType();
      Column.PhysicalColumn col = Column.PhysicalColumn.physical(
              fieldName,
              !notNullFieldSet.contains(fieldName) ? fieldType2dataType(type) : fieldType2dataType(type).notNull()
      );
      cols.add(col);
    }
    ResolvedSchema resolvedSchema = new ResolvedSchema(cols,
            Collections.emptyList(),
            primaryKeys == null || primaryKeys.isEmpty() ? null : UniqueConstraint.primaryKey("primaryKey", primaryKeys)
    );
    return resolvedSchema;
  }

  /**
   * 将数据转化成TableSchema
   *
   * @param databaseName
   * @param tableName
   * @param notnullFieldList
   * @param primaryKeys
   * @return
   * @throws TException
   */
  public TableSchema toFlinkTableSchema(String databaseName, String tableName, List<String> notnullFieldList, List<String> primaryKeys) throws TException {
    ResolvedSchema resolvedSchema = toFlinkResolvedSchema(
            databaseName,
            tableName,
            notnullFieldList,
            primaryKeys
    );
    return TableSchema.fromResolvedSchema(resolvedSchema);
  }

  /**
   * 将某个存储在hive catalog里的表转化成TypeInformation
   * @param databaseName
   * @param tableName
   * @return
   * @throws TException
   */
  public TypeInformation<Row> toFlinkTypeInformation(String databaseName, String tableName) throws TException {
    // 先获得hive表的schema
    List<FieldSchema> tableSchema = getTableSchema(databaseName, tableName);
    // 将schema转化成TypeInformation
    List<String> colNames = new LinkedList<>();
    List<TypeInformation<?>> typeInformationList = new LinkedList<>();
    for (int i = 0; i < tableSchema.size(); i++) {
      FieldSchema fieldSchema = tableSchema.get(i);
      colNames.add(fieldSchema.getName());
      typeInformationList.add(fieldType2typeInformation(fieldSchema.getType()));
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
    return rowTypeInfo;
  }


  public void close() {
    if (client != null) {
      client.close();
    }
  }
}