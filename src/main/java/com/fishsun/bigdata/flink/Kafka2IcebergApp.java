package com.fishsun.bigdata.flink;

import com.fishsun.bigdata.utils.KafkaUtils;
import com.fishsun.bigdata.utils.StreamUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.types.Row;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.fishsun.bigdata.utils.IcebergUtils.getTableLoader;
import static com.fishsun.bigdata.utils.ParamUtils.enhanceConfig;
import static com.fishsun.bigdata.utils.ParamUtils.parseConfig;
import static com.fishsun.bigdata.utils.ParamUtils.parseTableSchema;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/8 20:41
 * @Desc :
 */
public class Kafka2IcebergApp {
  private static final Logger logger = LoggerFactory.getLogger(Kafka2IcebergApp.class);

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


    // 3. 从 Kafka 读取数据
    logger.info("kafka source initializing");
    System.out.println("kafka source initializing");
    KafkaSource<Row> kafkaSource =
            KafkaUtils.getKafkaSource(paramMap);
    logger.info("successfully get kafka source");
    System.out.println("successfully get kafka source");
    DataStream<Row> kafkaStream = env.fromSource(
            kafkaSource,
            WatermarkStrategy.noWatermarks(),
            "Kafka Source"
    );
    logger.info("kafka source init finished");
    System.out.println("kafka source init finished");
    // 4. 定义 Iceberg 表的 TableSchema
    TableSchema tableSchema = parseTableSchema(
            paramMap
    );
    List<String> uniqueCols = new LinkedList<>();
    if (tableSchema.getPrimaryKey().isPresent()) {
      UniqueConstraint uniqueConstraint = tableSchema.getPrimaryKey().get();
      uniqueCols = uniqueConstraint.getColumns();
    }
    logger.info("table schema has been initialized");
    System.out.println("table schema has been initialized");
    // 5. 加载 Iceberg 表
    TableLoader tableLoader = getTableLoader(paramMap);
    logger.info("iceberg table has been loader successfully");
    System.out.println("iceberg table has been load successfully");
    // 6. 将数据写入 Iceberg 表
    if (uniqueCols.isEmpty()) {
      logger.info("iceberg sink will be OK without primary keys using append stream");
      System.out.println("iceberg sink will be OK without primary keys using append stream");
      FlinkSink.forRow(kafkaStream, tableSchema)
              .tableLoader(tableLoader)
              .tableSchema(tableSchema)
              .overwrite(false)
              .upsert(false)  // 开启 UPSERT 模式
              .append();
    } else {
      logger.info("iceberg sink will be OK with primary keys using upsert stream");
      System.out.println("iceberg sink will be OK with primary keys using upsert stream");
      logger.info("got {} primary keys", uniqueCols.size());
      System.out.println("got " + uniqueCols.size() + " primary keys");
      for (int i = 0; i < uniqueCols.size(); i++) {
        logger.info("primary key {} is {}", i + 1, uniqueCols.get(i));
        System.out.println("primary key " + (i + 1) + " is " + uniqueCols.get(i));
      }
      FlinkSink.forRow(kafkaStream, tableSchema)
              .tableLoader(tableLoader)
              .tableSchema(tableSchema)
              .overwrite(false)
              .upsert(true)  // 开启 UPSERT 模式
              .equalityFieldColumns(uniqueCols)
              .append();
      logger.info("sink has been OK");
      System.out.println("sink has been OK");
    }


    // 11. 启动 Flink 作业
    env.execute("Flink Kafka to Iceberg with Additional Fields");
  }

}
