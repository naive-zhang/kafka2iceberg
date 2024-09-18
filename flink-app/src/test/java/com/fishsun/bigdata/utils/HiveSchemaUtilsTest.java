package com.fishsun.bigdata.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.fishsun.bigdata.utils.IcebergUtils.HIVE_CATALOG_NS_NAME;
import static com.fishsun.bigdata.utils.IcebergUtils.HIVE_CATALOG_TBL_NAME;

/**
 * Created by IntelliJ IDEA.
 *
 * @Author : zhangxinsen
 * @create : 2024/9/16 16:45
 * @Desc :
 */
public class HiveSchemaUtilsTest {
    ;
    private static final String DATABASE_NAME = "test";
    private static final String TABLE_NAME = "t_busi_detail_flink_2";
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
    public void testGetTableSchema() {
        try {
            List<FieldSchema> schema =
                    HiveSchemaUtils.getInstance(paramMap)
                            .getTableSchema(DATABASE_NAME, TABLE_NAME);

            for (FieldSchema field : schema) {
                System.out.println("Column Name: " + field.getName());
                System.out.println("Column Type: " + field.getType());
                System.out.println("Comment: " + field.getComment());
                System.out.println("------------------------");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetTableParameters() {
        try {
            Map<String, String> tableParameters =
                    HiveSchemaUtils.getInstance(paramMap)
                            .getTableParameters(DATABASE_NAME, TABLE_NAME);
//      System.out.println(schema);
            for (Map.Entry<String, String> kvSet : tableParameters.entrySet()) {
                System.out.println(String.format("%s  : %s", kvSet.getKey(), kvSet.getValue()));
            }
        } catch (TException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testToFlinkResolvedSchema() throws TException {
        ResolvedSchema resolvedSchema = HiveSchemaUtils.getInstance(paramMap)
                .toFlinkResolvedSchema(
                        DATABASE_NAME, TABLE_NAME
                );
        System.out.println(resolvedSchema);
    }


    @Test
    public void testToFlinkTableSchema() throws TException {
        TableSchema tableSchema = HiveSchemaUtils.getInstance(paramMap)
                .toFlinkTableSchema(
                        DATABASE_NAME, TABLE_NAME
                );
        System.out.println(tableSchema);
    }

    @Test
    public void testToFlinkTypeInformation() throws TException {
        TypeInformation<Row> flinkTypeInformation = HiveSchemaUtils.getInstance(paramMap)
                .toFlinkTypeInformation(
                        DATABASE_NAME, TABLE_NAME
                );
        System.out.println(flinkTypeInformation);
    }

    @Test
    public void testToFlinkFieldName2typeInformation() throws TException {
        Map<String, TypeInformation<?>> flinkFieldName2typeInformation =
                HiveSchemaUtils.getInstance(paramMap).toFlinkFieldName2typeInformation(DATABASE_NAME, TABLE_NAME);
        for (Map.Entry<String, TypeInformation<?>> name2type : flinkFieldName2typeInformation.entrySet()) {
            System.out.println(name2type.getKey() + "   :    " + name2type.getValue());
        }
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
