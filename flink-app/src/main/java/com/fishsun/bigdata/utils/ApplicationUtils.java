package com.fishsun.bigdata.utils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.types.Row;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.thrift.TException;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.fishsun.bigdata.utils.IcebergUtils.HIVE_CATALOG_NS_NAME;
import static com.fishsun.bigdata.utils.IcebergUtils.HIVE_CATALOG_TBL_NAME;
import static com.fishsun.bigdata.utils.IcebergUtils.getTableLoader;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/16 23:28
 * @Desc :
 */
public class ApplicationUtils {
//  private static final Logger logger = LoggerFactory.getLogger(ApplicationUtils.class);

  /**
   * 构建kafkaSource
   *
   * @param env
   * @param paramMap
   * @return
   * @throws TException
   */
  public static DataStream<Row> setupKafkaSource(StreamExecutionEnvironment env, Map<String, String> paramMap) throws TException {
    String hiveTblName = String.format("%s-%s", paramMap.get(HIVE_CATALOG_NS_NAME), paramMap.get(HIVE_CATALOG_TBL_NAME));
    // 从 Kafka 读取数据
//    logger.info("kafka source initializing for " + hiveTblName);
    System.out.println("kafka source initializing for " + hiveTblName);
    KafkaSource<Row> kafkaSource =
            KafkaUtils.getKafkaSource(paramMap);
//    logger.info("successfully get kafka source for " + hiveTblName);
    System.out.println("successfully get kafka source for " + hiveTblName);
    DataStream<Row> kafkaStream = env.fromSource(
                    kafkaSource,
                    WatermarkStrategy.noWatermarks(),
                    "Kafka Source"
            ).uid(hiveTblName + "-kafka-source")
            .name(hiveTblName + "-kafka-source");
//    logger.info("kafka source init finished for" + hiveTblName);
    System.out.println("kafka source init finished for " + hiveTblName);
    return kafkaStream;
  }

  public static TableSchema setupTableSchema(Map<String, String> paramMap) throws TException {
    HiveSchemaUtils hiveSchemaUtils = HiveSchemaUtils.getInstance(
            paramMap
    );
    String hiveTblName = String.format("%s-%s", paramMap.get(HIVE_CATALOG_NS_NAME), paramMap.get(HIVE_CATALOG_TBL_NAME));
    TableSchema tableSchema = hiveSchemaUtils.toFlinkTableSchema(
            paramMap.get(HIVE_CATALOG_NS_NAME),
            paramMap.get(HIVE_CATALOG_TBL_NAME)
    );
//    logger.info(hiveTblName + " table schema has been initialized");
    System.out.println(hiveTblName + " table schema has been initialized");
    hiveSchemaUtils.close();
    return tableSchema;
  }

  public static TableLoader setupTableLoader(Map<String, String> paramMap) throws InterruptedException {
    String hiveTblName = String.format("%s-%s", paramMap.get(HIVE_CATALOG_NS_NAME), paramMap.get(HIVE_CATALOG_TBL_NAME));
    TableLoader tableLoader = getTableLoader(paramMap);
//    logger.info(hiveTblName + " iceberg table has been loader successfully");
    System.out.println(hiveTblName + " iceberg table has been load successfully");
    return tableLoader;
  }

  public static void setupSink(DataStream<Row> kafkaStream, TableSchema tableSchema, TableLoader tableLoader, Map<String, String> paramMap) {
    System.out.println("start setting up sink");
    System.out.println("kafka source: " + kafkaStream);
    System.out.println("table schema: " + tableSchema);
    System.out.println("table loader: " + tableLoader);
    String hiveTblName = String.format("%s-%s", paramMap.get(HIVE_CATALOG_NS_NAME), paramMap.get(HIVE_CATALOG_TBL_NAME));
    List<String> uniqueCols = new LinkedList<>();
    if (tableSchema.getPrimaryKey().isPresent()) {
      UniqueConstraint uniqueConstraint = tableSchema.getPrimaryKey().get();
      uniqueCols = uniqueConstraint.getColumns();
    }

    // 6. 将数据写入 Iceberg 表
    if (uniqueCols.isEmpty()) {
//      logger.info("iceberg sink will be OK without primary keys using append stream");
      System.out.println("iceberg sink will be OK without primary keys using append stream");
      FlinkSink.forRow(kafkaStream, tableSchema)
              .tableLoader(tableLoader)
              .tableSchema(tableSchema)
              .overwrite(false)
              .upsert(false)  // 开启 UPSERT 模式
              .append()
              .uid(hiveTblName + "-iceberg-sink")
              .name(hiveTblName + "-iceberg-sink");
    } else {
//      logger.info("iceberg sink will be OK with primary keys using upsert stream");
      System.out.println("iceberg sink will be OK with primary keys using upsert stream");
//      logger.info("got {} primary keys", uniqueCols.size());
      System.out.println("got " + uniqueCols.size() + " primary keys");
      for (int i = 0; i < uniqueCols.size(); i++) {
//        logger.info("primary key {} is {}", i + 1, uniqueCols.get(i));
        System.out.println("primary key " + (i + 1) + " is " + uniqueCols.get(i));
      }
      System.out.println("trying to prepare sink");
      System.out.println("preload the table");
      Table table = tableLoader.loadTable();
      System.out.println("table has been loaded successfully");
      System.out.println(table.name());
//      System.out.println(table.currentSnapshot().snapshotId());
      FlinkSink
              .forRow(kafkaStream, tableSchema)
              .tableLoader(tableLoader)
              .tableSchema(tableSchema)
              .overwrite(false)
              .upsert(true)  // 开启 UPSERT 模式
              .equalityFieldColumns(uniqueCols)
              .append()
              .uid(hiveTblName + "-iceberg-sink")
              .name(hiveTblName + "-iceberg-sink");
      System.out.println("sink has been prepared");
    }
//    logger.info("sink has been OK");
    System.out.println("sink has been OK");
  }

  public static void setupPipelines(StreamExecutionEnvironment env, Map<String, String> paramMap) throws InterruptedException,TException {
    try {


      // 1. 从 Kafka 读取数据
      DataStream<Row> kafkaStream = setupKafkaSource(
              env,
              paramMap
      );
      // 2. 加载 Iceberg 表
      TableLoader tableLoader = setupTableLoader(
              paramMap
      );

      // 3. 定义 Iceberg 表的 TableSchema
      TableSchema tableSchema = setupTableSchema(paramMap);
      // 4. 构建sink
      setupSink(
              kafkaStream,
              tableSchema,
              tableLoader,
              paramMap
      );
    } catch (Exception e) {
      System.out.println("got an err");
      System.out.println(e);
      System.out.println("message");
      System.out.println(e.getMessage());
      System.out.println("localized message");
      System.out.println(e.getLocalizedMessage());
      System.out.println("print trace ");
      e.printStackTrace();
      throw e;
    }
  }
}
