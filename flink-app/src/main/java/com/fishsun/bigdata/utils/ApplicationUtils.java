package com.fishsun.bigdata.utils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger logger = LoggerFactory.getLogger(ApplicationUtils.class);

    /**
     * 构建kafkaSource
     *
     * @param env
     * @param paramMap
     * @return
     * @throws TException
     */
    public static DataStream<RowData> setupKafkaSource(StreamExecutionEnvironment env, Map<String, String> paramMap) throws TException {
        String hiveTblName = String.format("%s-%s", paramMap.get(HIVE_CATALOG_NS_NAME), paramMap.get(HIVE_CATALOG_TBL_NAME));
        // 从 Kafka 读取数据
        logger.info("kafka source initializing for {}", hiveTblName);
        KafkaSource<RowData> kafkaSource =
                KafkaUtils.getKafkaSource(paramMap);
        logger.info("successfully get kafka source for {}", hiveTblName);
        DataStream<RowData> kafkaStream = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source"
                ).uid(hiveTblName + "-kafka-source")
                .name(hiveTblName + "-kafka-source")
                .setParallelism(1)
                .setMaxParallelism(1);
        logger.info("kafka source init finished for {}", hiveTblName);
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
        logger.info("{} table schema has been initialized", hiveTblName);
        hiveSchemaUtils.close();
        return tableSchema;
    }

    public static TableLoader setupTableLoader(Map<String, String> paramMap) throws InterruptedException {
        String hiveTblName = String.format("%s-%s", paramMap.get(HIVE_CATALOG_NS_NAME), paramMap.get(HIVE_CATALOG_TBL_NAME));
        TableLoader tableLoader = getTableLoader(paramMap);
        logger.info("{} iceberg table has been loader successfully", hiveTblName);
        return tableLoader;
    }

    public static void setupSink(DataStream<RowData> kafkaStream, TableSchema tableSchema, TableLoader tableLoader, Map<String, String> paramMap) {
        logger.info("start setting up sink");
        logger.info("kafka source: {}", kafkaStream);
        logger.info("table schema: {}", tableSchema);
        logger.info("table loader: {}", tableLoader);
        String hiveTblName = String.format("%s-%s", paramMap.get(HIVE_CATALOG_NS_NAME), paramMap.get(HIVE_CATALOG_TBL_NAME));
        List<String> uniqueCols = new LinkedList<>();
        if (tableSchema.getPrimaryKey().isPresent()) {
            UniqueConstraint uniqueConstraint = tableSchema.getPrimaryKey().get();
            uniqueCols = uniqueConstraint.getColumns();
        }

        // 6. 将数据写入 Iceberg 表
        FlinkSink.Builder builder = FlinkSink.forRowData(kafkaStream)
                .tableLoader(tableLoader)
                .tableSchema(tableSchema);
        Map<String, String> sinkConf = ParamUtils.getIcebergSinkParams(paramMap);
        for (Map.Entry<String, String> kvEntry : sinkConf.entrySet()) {
            builder.set(kvEntry.getKey(), kvEntry.getValue());
        }
        if (uniqueCols.isEmpty()) {
            logger.info("iceberg sink will be OK without primary keys using append stream");
            builder.overwrite(false)
                    .upsert(false)  // 开启 UPSERT 模式
                    .append()
                    .uid(hiveTblName + "-iceberg-sink")
                    .name(hiveTblName + "-iceberg-sink")
                    .setParallelism(1);

        } else {
            logger.info("iceberg sink will be OK with primary keys using upsert stream");
            logger.info("got {} primary keys", uniqueCols.size());
            for (int i = 0; i < uniqueCols.size(); i++) {
                logger.info("primary key {} is {}", i + 1, uniqueCols.get(i));
            }
            logger.info("trying to prepare sink");
            logger.info("preload the table");
            Table table = tableLoader.loadTable();
            logger.info("table has been loaded successfully");
            logger.info(table.name());
            builder.overwrite(false)
                    .upsert(true)  // 开启 UPSERT 模式
                    .equalityFieldColumns(uniqueCols)
                    .append()
                    .uid(hiveTblName + "-iceberg-sink")
                    .name(hiveTblName + "-iceberg-sink")
                    .setParallelism(1);
            logger.info("sink has been prepared");
        }
        logger.info("sink has been OK");
    }

    public static void setupPipelines(StreamExecutionEnvironment env, Map<String, String> paramMap) throws InterruptedException, TException {
        try {


            // 1. 从 Kafka 读取数据
            DataStream<RowData> kafkaStream = setupKafkaSource(
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
            logger.error("got an err");
            logger.error(e.toString());
            logger.error("message");
            logger.error(e.getMessage());
            logger.error("localized message");
            logger.error(e.getLocalizedMessage());
            logger.error("print trace ");
            e.printStackTrace();
            throw e;
        }
    }
}
