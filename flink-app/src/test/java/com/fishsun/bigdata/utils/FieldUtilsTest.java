package com.fishsun.bigdata.utils;

import org.apache.flink.table.types.logical.RowType;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.fishsun.bigdata.utils.IcebergUtils.HIVE_CATALOG_NS_NAME;
import static com.fishsun.bigdata.utils.IcebergUtils.HIVE_CATALOG_TBL_NAME;

public class FieldUtilsTest {
    private static final String argString = "iceberg.catalog.type=hive iceberg.uri=thrift://localhost:9083 hive.catalog.name=hive_iceberg hive.namespace.name=test hive.table.name=t_busi_detail_flink_2 bootstrap.servers=kafka:9092 topics=example group.id=flink-group source-database=test source-table=t_busi_detail fields.bid.is_primary_key=true fields.dt.is_primary_key=true fields.dt.ref=data.create_time";
    private static String[] args;
    private static Map<String, String> paramMap;

    private static final String insertRecord = "{\"data\":[{\"bid\":\"135189\",\"user_id\":\"1\",\"goods_id\":\"1\",\"goods_cnt\":\"1\",\"fee\":\"100.0\",\"collection_time\":\"2024-09-16 11:05:34\",\"order_time\":\"2024-09-16 11:05:34\",\"is_valid\":\"1\",\"create_time\":\"2024-09-16 11:05:34\",\"update_time\":\"2024-09-16 11:05:34\"}],\"database\":\"test\",\"es\":1726484734000,\"id\":15393,\"isDdl\":false,\"mysqlType\":{\"bid\":\"bigint(20)\",\"user_id\":\"int(11)\",\"goods_id\":\"int(11)\",\"goods_cnt\":\"int(11)\",\"fee\":\"decimal(16,4)\",\"collection_time\":\"datetime\",\"order_time\":\"datetime\",\"is_valid\":\"int(11)\",\"create_time\":\"datetime\",\"update_time\":\"datetime\"},\"old\":null,\"pkNames\":[\"bid\"],\"sql\":\"\",\"sqlType\":{\"bid\":-5,\"user_id\":4,\"goods_id\":4,\"goods_cnt\":4,\"fee\":3,\"collection_time\":93,\"order_time\":93,\"is_valid\":4,\"create_time\":93,\"update_time\":93},\"table\":\"t_busi_detail\",\"ts\":1726484734647,\"type\":\"INSERT\"}";

    @BeforeAll
    public static void setup() {
        args = argString.split("\\s+");
        paramMap = ParamUtils.parseConfig(args);
        ParamUtils.enhanceConfig(paramMap);
    }

    @Test
    public void testFieldType2logicalType() throws TException {
        HiveSchemaUtils schemaUtil = HiveSchemaUtils.getInstance(
                paramMap
        );
        RowType flinkRowType = schemaUtil.toFlinkRowType(
                paramMap.get(HIVE_CATALOG_NS_NAME),
                paramMap.get(HIVE_CATALOG_TBL_NAME)
        );
        System.out.println(flinkRowType);
        schemaUtil.close();
    }
}
