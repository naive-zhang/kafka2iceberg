package com.fishsun.bigdata.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/16 18:49
 * @Desc :
 */
public class ParamsUtilsTest {
    private static Logger logger = LoggerFactory.getLogger(ParamsUtilsTest.class);
    private static final String argString = "iceberg.catalog.type=hive iceberg.uri=thrift://hive:9083 hive.catalog.name=hive_iceberg hive.namespace.name=test hive.table.name=t_busi_detail_flink_2 bootstrap.servers=kafka:9092 topics=example group.id=flink-group source-database=test source-table=t_busi_detail fields.bid.is_primary_key=true fields.dt.is_primary_key=true fields.dt.ref=data.create_time";
    private static String[] args;
    private static Map<String, String> paramMap;

    @BeforeAll
    public static void setup() {
        args = argString.split("\\s+");
        paramMap = ParamUtils.parseConfig(args);
    }

    @Test
    public void testParseArgs() {
        Map<String, String> map = ParamUtils.parseConfig(args);
        for (Map.Entry<String, String> kvEntry : map.entrySet()) {
            logger.info("{}  : {}", kvEntry.getKey(), kvEntry.getValue());
        }
    }

    @Test
    public void testEnhancedArgs() {
        Map<String, String> paramMap = ParamUtils.parseConfig(args);
        ParamUtils.enhanceConfig(
                paramMap
        );
        for (Map.Entry<String, String> kvEntry : paramMap.entrySet()) {
            logger.info("{}  : {}", kvEntry.getKey(), kvEntry.getValue());
        }
    }

    @Test
    public void testGetPrimaryKeys() {
        List<String> primaryKeys = ParamUtils.getPrimaryKeys(paramMap);
        Assertions.assertTrue(primaryKeys.size() == 2);
    }

    @Test
    void testGetNotNullableCols() {
        List<String> notNullableCols = ParamUtils.getNotNullableCols(paramMap);
        Assertions.assertTrue(notNullableCols.isEmpty());
    }

    @Test
    void testGetColWithRef() {
        Map<String, String> colWithRef = ParamUtils.getColWithRef(paramMap);
        Assertions.assertTrue(colWithRef.size() == 1);
    }
}
