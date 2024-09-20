package com.fishsun.bigdata.utils;

import org.apache.flink.table.types.logical.RowType;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.fishsun.bigdata.utils.IcebergUtils.HIVE_CATALOG_NS_NAME;
import static com.fishsun.bigdata.utils.IcebergUtils.HIVE_CATALOG_TBL_NAME;

public class FieldUtilsTest {
    private static final Logger logger = LoggerFactory.getLogger(FieldUtilsTest.class);
    private static final String argString = "iceberg.catalog.type=hive iceberg.uri=thrift://localhost:9083 hive.catalog.name=hive_iceberg hive.namespace.name=test hive.table.name=t_busi_detail_flink_2 bootstrap.servers=kafka:9092 topics=example group.id=flink-group source-database=test source-table=t_busi_detail fields.bid.is_primary_key=true fields.dt.is_primary_key=true fields.dt.ref=data.create_time";
    private static String[] args;
    private static Map<String, String> paramMap;

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
        logger.info(flinkRowType.toString());
        schemaUtil.close();
    }
}
