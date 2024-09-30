package com.fishsun.bigdata;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fishsun.bigdata.utils.ApplicationUtils;
import com.fishsun.bigdata.utils.DateTimeUtils;
import com.fishsun.bigdata.utils.HiveSchemaUtils;
import com.fishsun.bigdata.utils.ParamUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.fishsun.bigdata.utils.IcebergUtils.HIVE_CATALOG_NS_NAME;
import static com.fishsun.bigdata.utils.IcebergUtils.HIVE_CATALOG_TBL_NAME;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/15 0:30
 * @Desc :
 */

public class DeserializedSchema implements KafkaRecordDeserializationSchema<RowData> {

  private static final Logger logger = LoggerFactory.getLogger(DeserializedSchema.class);

  private final Map<String, String> paramMap;
  private final Map<String, String> colWithRef;
  private final RowType rowType;
  private final Set<String> notNullColSet;
  private final Set<String> dateFieldSet;
  private String database;
  private String table;


  public DeserializedSchema(
          Map<String, String> paramMap
  ) throws TException {
    this.paramMap = paramMap;
    if (this.paramMap.containsKey("source-database")) {
      database = this.paramMap.get("source-database");
      logger.info("kafka canal database: {}", database);
    }
    if (this.paramMap.containsKey("source-table")) {
      table = this.paramMap.get("source-table");
      logger.info("kafka canal table: {}", table);
    }
    HiveSchemaUtils schemaUtil = HiveSchemaUtils.getInstance(
            paramMap
    );
    colWithRef = ParamUtils.getColWithRef(paramMap);
    rowType = schemaUtil.toFlinkRowType(
            paramMap.get(HIVE_CATALOG_NS_NAME),
            paramMap.get(HIVE_CATALOG_TBL_NAME)
    );
    notNullColSet = schemaUtil.getNotNullColSet(
            paramMap.get(HIVE_CATALOG_NS_NAME),
            paramMap.get(HIVE_CATALOG_TBL_NAME)
    );

    dateFieldSet = schemaUtil.getDateTypeFieldSet(
            paramMap.get(HIVE_CATALOG_NS_NAME),
            paramMap.get(HIVE_CATALOG_TBL_NAME)
    );
  }

  private static final long serialVersionUID = 1L;

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Override
  public TypeInformation<RowData> getProducedType() {

    // 解析定义好的类型
    return InternalTypeInfo.of(rowType);
  }


  @Override
  public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> out) throws IOException {
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
      return;
    }
    if (jsonNode.get("isDdl").asText().trim().equalsIgnoreCase("true")) {
      try {
        ApplicationUtils.stop();
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }


    setupRow(
            out,
            jsonNode,
            record.offset(),
            record.partition()
    );
  }


  public void setupRow(Collector<RowData> out,
                       JsonNode jsonNode,
                       Long offset,
                       int partitionIdx) {
    // 获得相应的数据类型
    String type = jsonNode.get("type").asText();
    boolean isCdcDelete = "DELETE".equalsIgnoreCase(type);

    // 获取 data 数组
    JsonNode dataArray = jsonNode.get("data");

    if (dataArray != null && dataArray.isArray()) {
      for (JsonNode dataNode : dataArray) {
        GenericRowData rowData = new GenericRowData(rowType.getFieldCount());
        List<String> fieldNames = rowType.getFieldNames();
        List<RowType.RowField> fields = rowType.getFields();
        for (int i = 0; i < rowType.getFieldCount(); i++) {
          // logger.info("parsing {} with type {}, and the root type is {}"
          // , fieldNames.get(i), fields.get(i).getType().toString(),
          // fields.get(i).getType().getTypeRoot());
          String fieldName = fieldNames.get(i);
          RowType.RowField rowField = fields.get(i);
          LogicalType fieldType = rowField.getType();
          String ref = colWithRef.getOrDefault(fieldName, "data." + fieldName);
          String key = ref.substring(5);
          if (dataNode.has(key)) {
            JsonNode node = dataNode.get(key);
            if (node.asText().trim().equalsIgnoreCase("null")) {
              if (notNullColSet.contains(fieldName)) {
                throw new IllegalArgumentException(fieldName + " should not be null, " + jsonNode);
              } else {
                rowData.setField(i, null);
              }
            } else if (fieldType.getTypeRoot().equals(LogicalTypeRoot.VARCHAR)) {
              rowData.setField(i, StringData.fromString(node.asText()));
            } else if (fieldType.getTypeRoot().equals(LogicalTypeRoot.BIGINT)) {
              rowData.setField(i, node.asLong());
            } else if (fieldType.getTypeRoot().equals(LogicalTypeRoot.INTEGER)) {
              if (dateFieldSet != null && !dateFieldSet.isEmpty() && dateFieldSet.contains(fieldName)) {
                LocalDate localDate = DateTimeUtils.parseString2localDate(node.asText());
                int daysSinceEpoch = (int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), localDate);
                rowData.setField(i, daysSinceEpoch);
              } else {
                rowData.setField(i, node.asInt());
              }
            } else if (fieldType.getTypeRoot().equals(LogicalTypeRoot.BOOLEAN)) {
              rowData.setField(i, node.asBoolean());
            } else if (fieldType.getTypeRoot().equals(LogicalTypeRoot.DECIMAL)) {
              DecimalType decimalType = (DecimalType) fieldType;
              rowData.setField(i,
                      DecimalData.fromBigDecimal(
                              new BigDecimal(node.asText()),
                              decimalType.getPrecision(),
                              decimalType.getScale()
                      ));
            } else if (fieldType.getTypeRoot().equals(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)) {
              rowData.setField(i, TimestampData.fromLocalDateTime(DateTimeUtils.parseStringToLocalDateTime(node.asText())));
            } else if (fieldType.getTypeRoot().equals(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
              rowData.setField(i, TimestampData.fromInstant(DateTimeUtils.parseStringToLocalDateTime(node.asText())
                              .atZone(
                                      ZoneId.of("Asia/Shanghai")
                              ).toInstant())
//                              .toInstant(ZoneOffset.UTC))
              )
              ;
            } else if (fieldType.getTypeRoot().equals(LogicalTypeRoot.DATE)) {
              LocalDate localDate = DateTimeUtils.parseString2localDate(node.asText());
              int daysSinceEpoch = (int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), localDate);
              rowData.setField(i, daysSinceEpoch);
            }
          } else if (fieldName.trim().equalsIgnoreCase("is_cdc_delete")) {
            rowData.setField(i, isCdcDelete);
          } else if (fieldName.trim().equalsIgnoreCase("offset")) {
            rowData.setField(i, offset);
          } else if (fieldName.trim().equalsIgnoreCase("partition_idx")) {
            rowData.setField(i, partitionIdx);
          } else if (fieldName.trim().equalsIgnoreCase("ts")) {
            rowData.setField(i, jsonNode.get("ts").asLong());
          } else if (fieldName.trim().equalsIgnoreCase("es")) {
            rowData.setField(i, jsonNode.get("es").asLong());
          } else if (fieldName.trim().equalsIgnoreCase("CommitTs") || fieldName.trim().equalsIgnoreCase("commit_ts")) {
            rowData.setField(i, jsonNode.get("_tidb").get("commitTs").asLong());
          } else if (fieldName.trim().equalsIgnoreCase("ingestion_tm")) {
            rowData.setField(i, TimestampData.fromInstant(DateTimeUtils.parseStringToLocalDateTime(
                                    DateTimeUtils.getBeijingTimeNow()
                            )
                            .atZone(
                                    ZoneId.of("Asia/Shanghai")
                            ).toInstant())
//                                        .toInstant(ZoneOffset.UTC)
//                                )
            );
          } else if (!notNullColSet.contains(fieldName)) {
            rowData.setField(i, null);
          } else {
            throw new IllegalArgumentException("field " + fieldName + ", json not got it with ref " + key + ", and it's not nullable");
          }
        }
        out.collect(rowData);
      }
    }
  }

}