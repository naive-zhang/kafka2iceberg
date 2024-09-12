package com.fishsun.bigdata.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fishsun.bigdata.utils.KafkaUtils;
import com.fishsun.bigdata.utils.StreamUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Map;

import static com.fishsun.bigdata.utils.IcebergUtils.getTableLoader;
import static com.fishsun.bigdata.utils.ParamUtils.enhanceConfig;
import static com.fishsun.bigdata.utils.ParamUtils.parseConfig;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/8 20:41
 * @Desc :
 */
public class Kafka2IcebergApp {
  public static void main(String[] args) throws Exception {
    Map<String, String> paramMap = parseConfig(args);
    enhanceConfig(paramMap);
    // 1. 创建 Flink 执行环境
    // local=false(默认) --> 集群环境
    // local=true(本地webUI) --> 本地环境 rest.port配置端口
    StreamExecutionEnvironment env =
            StreamUtils.getStreamEnv(paramMap);

    // 2. 配置 Kafka Source
    // bootstrap.servers配置kafka
    // topics配置topic
    // group.id配置group
    KafkaSource<Row> kafkaSource =
            KafkaUtils.getKafkaSource(paramMap);

    // 3. 从 Kafka 读取数据
    DataStream<Row> kafkaStream = env.fromSource(
            kafkaSource,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
    );
    // 4. 定义 Iceberg 表的 TableSchema
    TableSchema tableSchema = TableSchema.builder()
            .field("bid", DataTypes.BIGINT().notNull())  // bid 为主键，并且 NOT NULL
            .field("user_id", DataTypes.BIGINT())  // 其他字段可为 NULL
            .field("goods_id", DataTypes.INT())
            .field("goods_cnt", DataTypes.BIGINT())
            .field("fee", DataTypes.DECIMAL(16, 4))
            .field("collection_time", DataTypes.TIMESTAMP(6))
            .field("order_time", DataTypes.TIMESTAMP(6))
            .field("is_valid", DataTypes.INT())
            .field("create_time", DataTypes.TIMESTAMP(6))
            .field("update_time", DataTypes.TIMESTAMP(6))
            .field("offset", DataTypes.BIGINT())
            .field("partition_idx", DataTypes.INT())
            .field("is_cdc_delete", DataTypes.BOOLEAN())
            .field("dt", DataTypes.DATE())
            .primaryKey("bid")
            .build();

    // 5. 加载 Iceberg 表
    TableLoader tableLoader = getTableLoader(paramMap);

    // 6. 将数据写入 Iceberg 表
    FlinkSink.forRow(kafkaStream, tableSchema)
            .tableLoader(tableLoader)
            .tableSchema(tableSchema)
            .overwrite(false)
            .upsert(true)  // 开启 UPSERT 模式
            .equalityFieldColumns(Arrays.asList("dt", "bid"))
            .append();

    // 11. 启动 Flink 作业
    env.execute("Flink Kafka to Iceberg with Additional Fields");
  }

  // 自定义反序列化类
  public static class CustomCanalDeserializationSchema implements KafkaRecordDeserializationSchema<Row> {
    private static final long serialVersionUID = 1L;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TypeInformation<Row> getProducedType() {
      // 使用 DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) 表示的类型
      LogicalType timestampWithLocalZoneType = DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).getLogicalType();

      // 将 DataTypes 转换为对应的 TypeInformation
//      TypeConversions.fromLogicalToDataType(timestampWithLocalZoneType).
//      TypeInformation<?> typeInfo = TypeConversions.fromLogicalToDataType(timestampWithLocalZoneType).getConversionClass();


      // 定义字段类型和名称
      TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{
              Types.LONG,          // bid
              Types.LONG,          // user_id
              Types.INT,           // goods_id
              Types.LONG,          // goods_cnt
              Types.BIG_DEC,       // fee
              Types.LOCAL_DATE_TIME,      // collection_time
              Types.LOCAL_DATE_TIME,      // order_time
              Types.INT,           // is_valid
              Types.LOCAL_DATE_TIME,      // create_time
              Types.LOCAL_DATE_TIME,      // update_time
              Types.LONG,          // offset
              Types.INT,           // partition_idx
              Types.BOOLEAN,       // is_cdc_delete
              Types.LOCAL_DATE       // dt
      };

      String[] fieldNames = new String[]{
              "bid",
              "user_id",
              "goods_id",
              "goods_cnt",
              "fee",
              "collection_time",
              "order_time",
              "is_valid",
              "create_time",
              "update_time",
              "offset",
              "partition_idx",
              "is_cdc_delete",
              "dt"
      };

      return new RowTypeInfo(fieldTypes, fieldNames);
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

      if (!jsonNode.get("database").asText().equals("test") || !jsonNode.get("table").asText().equals("t_busi_detail")) {
        return;
      }

      String type = jsonNode.get("type").asText();

      boolean isCdcDelete = "DELETE".equalsIgnoreCase(type);

      // 获取 data 数组
      JsonNode dataArray = jsonNode.get("data");

      if (dataArray != null && dataArray.isArray()) {
        for (JsonNode dataNode : dataArray) {
          Row row = new Row(14);
          // 提取字段
          row.setField(0, dataNode.get("bid").asLong());
          row.setField(1, dataNode.get("user_id").asLong());
          row.setField(2, dataNode.get("goods_id").asInt());
          row.setField(3, dataNode.get("goods_cnt").asLong());
          row.setField(4, new BigDecimal(dataNode.get("fee").asText()));

          // collection_time
          row.setField(5, parseTimestamp(dataNode.get("collection_time").asText()).toLocalDateTime());
          // order_time
          row.setField(6, parseTimestamp(dataNode.get("order_time").asText()).toLocalDateTime());
          // is_valid
          row.setField(7, dataNode.get("is_valid").asInt());
          // create_time
          Timestamp createTime = parseTimestamp(dataNode.get("create_time").asText());
          row.setField(8, createTime.toLocalDateTime());
          // update_time
          row.setField(9, parseTimestamp(dataNode.get("update_time").asText())
                  .toLocalDateTime());

          // 设置 offset 和 partition_idx
          row.setField(10, record.offset());
          row.setField(11, record.partition());
          // 设置 is_cdc_delete
          row.setField(12, isCdcDelete);
          // 设置 dt（基于 create_time）
          LocalDate dt = new Date(createTime.getTime()).toLocalDate();
          row.setField(13, dt);

          out.collect(row);
        }
      }
    }

    // 辅助方法：解析时间字符串为 Timestamp
    private Timestamp parseTimestamp(String timeStr) {
      return Timestamp.valueOf(timeStr);
    }
  }
}
