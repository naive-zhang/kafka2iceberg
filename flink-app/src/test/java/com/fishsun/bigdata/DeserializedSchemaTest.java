package com.fishsun.bigdata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fishsun.bigdata.utils.DateTimeUtils;
import com.fishsun.bigdata.utils.HiveSchemaUtils;
import com.fishsun.bigdata.utils.ParamUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
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
 * @create : 2024/9/16 18:59
 * @Desc :
 */
public class DeserializedSchemaTest {
    private static final Logger logger = LoggerFactory.getLogger(DeserializedSchemaTest.class);
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
    public void testGetName2typeInfo() throws TException {
        HiveSchemaUtils schemaUtil = HiveSchemaUtils.getInstance(
                paramMap
        );
        Map<String, TypeInformation<?>> name2typeInfo = schemaUtil.toFlinkFieldName2typeInformation(
                paramMap.get(HIVE_CATALOG_NS_NAME),
                paramMap.get(HIVE_CATALOG_TBL_NAME)
        );
        for (Map.Entry<String, TypeInformation<?>> name2typeEntry : name2typeInfo.entrySet()) {
            logger.info("{}:{}", name2typeEntry.getKey(), name2typeEntry.getValue());
        }
        schemaUtil.close();
    }

    @Test
    public void deserializedRow() throws TException, JsonProcessingException {
        HiveSchemaUtils schemaUtil = HiveSchemaUtils.getInstance(
                paramMap
        );
        TypeInformation<Row> flinkTypeInformation = schemaUtil.toFlinkTypeInformation(
                paramMap.get(HIVE_CATALOG_NS_NAME),
                paramMap.get(HIVE_CATALOG_TBL_NAME)
        );
        String[] fieldNames = ((RowTypeInfo) flinkTypeInformation).getFieldNames();
        TypeInformation<?>[] fieldTypes = ((RowTypeInfo) flinkTypeInformation).getFieldTypes();
        schemaUtil.close();
        Map<String, String> colWithRef = ParamUtils.getColWithRef(paramMap);
        List<String> notNullableCols = ParamUtils.getNotNullableCols(paramMap);
        Set<String> notNullColSet = new HashSet();
        notNullColSet.addAll(notNullableCols);
        ObjectMapper objectMapper = new ObjectMapper();

        byte[] value = insertRecord.getBytes(StandardCharsets.UTF_8);
        if (value == null) {
            return;
        }

        for (int i = 0; i < fieldNames.length; i++) {
            logger.info("{} col {} with type {}", i + 1, fieldNames[i], fieldTypes[i]);
        }

        // 解析 Canal-JSON 消息
        String jsonString = new String(value, StandardCharsets.UTF_8);
        JsonNode jsonNode = objectMapper.readTree(jsonString);
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
                            row.setField(i, DateTimeUtils.parseStringToLocalDateTime(node.asText()));
                        } else if (typeInformation.equals(Types.LOCAL_DATE)) {
                            row.setField(i, DateTimeUtils.parseString2localDate(node.asText()));
                        }
                    } else if (fieldName.trim().equalsIgnoreCase("is_cdc_delete")) {
                        row.setField(i, isCdcDelete);
                    } else if (fieldName.trim().equalsIgnoreCase("offset")) {
                        row.setField(i, 0);
                    } else if (fieldName.trim().equalsIgnoreCase("partition_idx")) {
                        row.setField(i, 0);
                    } else if (fieldName.trim().equalsIgnoreCase("ts")) {
                        row.setField(i, jsonNode.get("ts").asLong());
                    } else if (fieldName.trim().equalsIgnoreCase("es")) {
                        row.setField(i, jsonNode.get("es").asLong());
                    } else if (fieldName.trim().equalsIgnoreCase("CommitTs")) {
                        row.setField(i, jsonNode.get("CommitTs").asLong());
                    } else if (!notNullColSet.contains(fieldName)) {
                        row.setField(i, null);
                    } else {
                        throw new IllegalArgumentException("field " + fieldName + ", json not got it with ref " + key + ", and it's not nullable");
                    }
                }
                logger.info(row.toString());
            }
        }
    }

    @Test
    public void testLocalDatetime2Instant() {
        String tm = "2024-09-10 12:21:14";
        Instant instant = DateTimeUtils.parseStringToLocalDateTime(tm)
//            .atZone(ZoneId.of("Asia/Shanghai"))
                .toInstant(ZoneOffset.UTC);
        logger.info(instant.toString());
    }
}
