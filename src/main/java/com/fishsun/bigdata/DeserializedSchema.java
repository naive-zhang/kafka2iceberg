package com.fishsun.bigdata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

import static com.fishsun.bigdata.utils.FieldUtils.fieldType2typeInformation;
import static com.fishsun.bigdata.utils.ParamUtils.filterTableSchemaConfig;
import static com.fishsun.bigdata.utils.ParamUtils.parseTypeInformation;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/15 0:30
 * @Desc :
 */

public class DeserializedSchema implements KafkaRecordDeserializationSchema<Row> {

  private final Map<String, String> paramMap;
  private String database;
  private String table;

  public DeserializedSchema(
          Map<String, String> paramMap
  ) {
    this.paramMap = paramMap;
    if (this.paramMap.containsKey("source-database")) {
      database = this.paramMap.get("source-database");
      System.out.println("kafka canal database: " + database);
    }
    if (this.paramMap.containsKey("source-table")) {
      table = this.paramMap.get("source-table");
      System.out.println("kafka canal table: " + table);
    }
  }

  private static final long serialVersionUID = 1L;

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public TypeInformation<Row> getProducedType() {

    // 解析定义好的类型
    return parseTypeInformation(paramMap);
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


  public static void setupRow(Collector<Row> out,
                              JsonNode jsonNode,
                              Long offset,
                              int partitionIdx,
                              Map<String, String> paramMap) {
    // 获得相应的数据类型
    String type = jsonNode.get("type").asText();
    boolean isCdcDelete = "DELETE".equalsIgnoreCase(type);

    // 获取 data 数组
    JsonNode dataArray = jsonNode.get("data");
    Map<Integer, Map<String, String>> seq2fieldInfo = filterTableSchemaConfig(paramMap);

    if (dataArray != null && dataArray.isArray()) {
      for (JsonNode dataNode : dataArray) {
        Row row = new Row(seq2fieldInfo.size());
        for (int i = 0; i < seq2fieldInfo.size(); i++) {
          Map<String, String> fieldInfo = seq2fieldInfo.get(i + 1);
          String fieldName = fieldInfo.get("name");
          TypeInformation typeInformation = fieldType2typeInformation(fieldInfo.get("type"));
          String ref = fieldInfo.get("ref");
          String key = ref.substring(5);
          if (dataNode.has(key)) {
            JsonNode node = dataNode.get(key);
            if (typeInformation.equals(Types.STRING)) {
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
              row.setField(i, parseTimestamp(node.asText()).toLocalDateTime());
            } else if (typeInformation.equals(Types.LOCAL_TIME)) {
              row.setField(i, new Date(parseTimestamp(node.asText()).getTime()).toLocalDate());
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
          } else if (fieldName.trim().equalsIgnoreCase("CommitTs")) {
            row.setField(i, jsonNode.get("CommitTs").asLong());
          } else if (fieldInfo.get("is_nullable").trim().equalsIgnoreCase("true")) {
            row.setField(i, null);
          } else {
            throw new IllegalArgumentException("field " + fieldName + ", json not got it with ref " + key + ", and it's not nullable");
          }
        }
        out.collect(row);
      }
    }
  }

  // 辅助方法：解析时间字符串为 Timestamp
  private static Timestamp parseTimestamp(String timeStr) {
    return Timestamp.valueOf(timeStr);
  }

  public static void main(String[] args) throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    String jsonString = "{\"data\":[{\"bid\":\"119950\",\"user_id\":\"1\",\"goods_id\":\"1\",\"goods_cnt\":\"1\",\"fee\":\"100.0\",\"collection_time\":\"2024-09-15 07:52:00\",\"order_time\":\"2024-09-15 07:52:00\",\"is_valid\":\"1\",\"sign_time\":null,\"create_time\":\"2024-09-15 07:52:00\",\"update_time\":\"2024-09-15 07:52:00\"}],\"database\":\"test\",\"es\":1726386720000,\"id\":346,\"isDdl\":false,\"mysqlType\":{\"bid\":\"bigint(20)\",\"user_id\":\"int(11)\",\"goods_id\":\"int(11)\",\"goods_cnt\":\"int(11)\",\"fee\":\"decimal(16,4)\",\"collection_time\":\"datetime\",\"order_time\":\"datetime\",\"is_valid\":\"int(11)\",\"sign_time\":\"datetime\",\"create_time\":\"datetime\",\"update_time\":\"datetime\"},\"old\":null,\"pkNames\":[\"bid\"],\"sql\":\"\",\"sqlType\":{\"bid\":-5,\"user_id\":4,\"goods_id\":4,\"goods_cnt\":4,\"fee\":3,\"collection_time\":93,\"order_time\":93,\"is_valid\":4,\"sign_time\":93,\"create_time\":93,\"update_time\":93},\"table\":\"t_busi_detail\",\"ts\":1726386720538,\"type\":\"INSERT\"}";
    JsonNode jsonNode = objectMapper.readTree(jsonString);
    System.out.println(jsonNode.get("database").asText());
    System.out.println(jsonNode.has("testf"));
  }
}