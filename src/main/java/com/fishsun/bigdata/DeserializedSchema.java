package com.fishsun.bigdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fishsun.bigdata.utils.DateTimeUtils;
import com.fishsun.bigdata.utils.HiveSchemaUtils;
import com.fishsun.bigdata.utils.ParamUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.thrift.TException;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.fishsun.bigdata.utils.IcebergUtils.HIVE_CATALOG_NS_NAME;
import static com.fishsun.bigdata.utils.IcebergUtils.HIVE_CATALOG_TBL_NAME;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/15 0:30
 * @Desc :
 */

public class DeserializedSchema implements KafkaRecordDeserializationSchema<Row> {

  private final Map<String, String> paramMap;
  private final String[] fieldNames;
  private final TypeInformation<?>[] fieldTypes;
  private final Map<String, String> colWithRef;
  private final TypeInformation<Row> flinkTypeInformation;
  private Set<String> notNullColSet;
  private String database;
  private String table;


  public DeserializedSchema(
          Map<String, String> paramMap
  ) throws TException {
    this.paramMap = paramMap;
    if (this.paramMap.containsKey("source-database")) {
      database = this.paramMap.get("source-database");
      System.out.println("kafka canal database: " + database);
    }
    if (this.paramMap.containsKey("source-table")) {
      table = this.paramMap.get("source-table");
      System.out.println("kafka canal table: " + table);
    }
    HiveSchemaUtils schemaUtil = HiveSchemaUtils.buildFromParamMap(
            paramMap
    );
    flinkTypeInformation = schemaUtil.toFlinkTypeInformation(
            paramMap.get(HIVE_CATALOG_NS_NAME),
            paramMap.get(HIVE_CATALOG_TBL_NAME)
    );
    fieldNames = ((RowTypeInfo) flinkTypeInformation).getFieldNames();
    fieldTypes = ((RowTypeInfo) flinkTypeInformation).getFieldTypes();
    schemaUtil.close();
    colWithRef = ParamUtils.getColWithRef(paramMap);
    List<String> notNullableCols = ParamUtils.getNotNullableCols(paramMap);
    notNullColSet = new HashSet<>();
    notNullColSet.addAll(notNullableCols);
  }

  private static final long serialVersionUID = 1L;

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public TypeInformation<Row> getProducedType() {

    // 解析定义好的类型
    return flinkTypeInformation;
  }


  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Row> out) throws IOException {
    byte[] value = record.value();
    if (value == null) {
      return;
    }

    // 解析 Canal-JSON 消息
    String jsonString = new String(value, StandardCharsets.UTF_8);
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    // 判断当前消息是否属于相应的数据
    if (this.database == null || this.table == null) {
      throw new IllegalArgumentException("canal-kafka source database or table not config");
    }

    // 忽略非当前表的cdc数据
    if (!jsonNode.get("database").asText().equals(database) || !jsonNode.get("table").asText().equals(table)) {
      return;
    }
    // 忽略查询的内容
    if (jsonNode.get("type").asText().trim().equalsIgnoreCase("query")) {
//      System.out.println("not hit");
      return;
    }
    System.out.println("数据被命中");
    if (jsonNode.get("isDdl").asText().trim().equalsIgnoreCase("true")) {
      // TODO 判断此处的内容如何处理
    }


    setupRow(
            out,
            jsonNode,
            record.offset(),
            record.partition(),
            paramMap
    );
  }


  public void setupRow(Collector<Row> out,
                       JsonNode jsonNode,
                       Long offset,
                       int partitionIdx,
                       Map<String, String> paramMap) {
    // 获得相应的数据类型
    String type = jsonNode.get("type").asText();
    boolean isCdcDelete = "DELETE".equalsIgnoreCase(type);

    // 获取 data 数组
    JsonNode dataArray = jsonNode.get("data");

    if (dataArray != null && dataArray.isArray()) {
      for (JsonNode dataNode : dataArray) {
        Row row = new Row(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
          String fieldName = fieldNames[i];
          TypeInformation typeInformation = fieldTypes[i];
          String ref = colWithRef.getOrDefault(fieldName, "data." + fieldName);
          String key = ref.substring(5);
          if (dataNode.has(key)) {
            JsonNode node = dataNode.get(key);
            if (node.asText().trim().equalsIgnoreCase("null")) {
              if (notNullColSet.contains(fieldName)) {
                throw new IllegalArgumentException(fieldName + " should not be null, " + jsonNode);
              } else {
                row.setField(i, null);
              }
            } else if (typeInformation.equals(Types.STRING)) {
              row.setField(i, node.asText());
            } else if (typeInformation.equals(Types.LONG)) {
              row.setField(i, node.asLong());
            } else if (typeInformation.equals(Types.INT)) {
              row.setField(i, node.asInt());
            } else if (typeInformation.equals(Types.BOOLEAN)) {
              row.setField(i, node.asBoolean());
            } else if (typeInformation.equals(Types.BIG_DEC)) {
              row.setField(i, new BigDecimal(node.asText()));
            } else if (typeInformation.equals(Types.LOCAL_DATE_TIME)) {
              row.setField(i, DateTimeUtils.parseStringToLocalDateTime(node.asText()));
            } else if (typeInformation.equals(Types.LOCAL_DATE)) {
              row.setField(i, DateTimeUtils.parseString2localDate(node.asText()));
            }
          } else if (fieldName.trim().equalsIgnoreCase("is_cdc_delete")) {
            row.setField(i, isCdcDelete);
          } else if (fieldName.trim().equalsIgnoreCase("offset")) {
            row.setField(i, offset);
          } else if (fieldName.trim().equalsIgnoreCase("partition_idx")) {
            row.setField(i, partitionIdx);
          } else if (fieldName.trim().equalsIgnoreCase("ts")) {
            row.setField(i, jsonNode.get("ts").asLong());
          } else if (fieldName.trim().equalsIgnoreCase("es")) {
            row.setField(i, jsonNode.get("es").asLong());
          } else if (fieldName.trim().equalsIgnoreCase("CommitTs") || fieldName.trim().equalsIgnoreCase("commit_ts")) {
            row.setField(i, jsonNode.get("CommitTs").asLong());
          } else if (!notNullColSet.contains(fieldName)) {
            row.setField(i, null);
          } else {
            throw new IllegalArgumentException("field " + fieldName + ", json not got it with ref " + key + ", and it's not nullable");
          }
        }
        out.collect(row);
      }
    }
  }

}